// Program.cs — .NET 8

using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Polly;

// ===================== TOP-LEVEL APP WIRES FIRST =====================

var builder = WebApplication.CreateBuilder(args);

// EF Core (sample)
builder.Services.AddDbContext<AppDbContext>(o => o.UseInMemoryDatabase("app"));

// Bind Service Bus config (from appsettings: "ServiceBus": { ... })
builder.Services
    .AddOptions<MessagingConfig>()
    .Bind(builder.Configuration.GetSection("ServiceBus"))
    .PostConfigure(options =>
    {
        // sensible defaults/fallbacks
        if (string.IsNullOrWhiteSpace(options.Queue)) options.Queue = "orders";
    });

// JSON options
builder.Services.AddSingleton(new JsonSerializerOptions(JsonSerializerDefaults.Web)
{
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    PropertyNameCaseInsensitive = true
});

// ServiceBusClient (ConnectionString OR FQDN + Managed Identity)
builder.Services.AddSingleton(sp =>
{
    var cfg = sp.GetRequiredService<IOptions<MessagingConfig>>().Value;

    if (!string.IsNullOrWhiteSpace(cfg.ConnectionString))
        return new ServiceBusClient(cfg.ConnectionString);

    if (string.IsNullOrWhiteSpace(cfg.Fqdn))
        throw new InvalidOperationException(
            "Set either ServiceBus:ConnectionString or ServiceBus:Fqdn in configuration.");

    return new ServiceBusClient(cfg.Fqdn, new DefaultAzureCredential());
});

// Background workers
builder.Services.AddHostedService<OutboxPublisher>();
builder.Services.AddHostedService<OrderQueueConsumer>();

var app = builder.Build();

// Create order endpoint (DB write + Outbox append)
app.MapPost("/orders", async (OrderDto dto, AppDbContext db, JsonSerializerOptions json) =>
{
    var order = new Order { CustomerId = dto.CustomerId, Amount = dto.Amount };
    await db.Orders.AddAsync(order);

    var outbox = new OutboxMessage
    {
        Type = "OrderCreated",
        Payload = JsonSerializer.Serialize(new { order.Id, order.CustomerId, order.Amount }, json),
        CorrelationId = order.Id.ToString()
    };
    await db.Outbox.AddAsync(outbox);

    await db.SaveChangesAsync();
    return Results.Created($"/orders/{order.Id}", new { order.Id });
});

// Optional: DLQ re-drive
app.MapPost("/admin/retry-dlq", async (ServiceBusClient client, IOptions<MessagingConfig> cfgOpt) =>
{
    var cfg = cfgOpt.Value;
    var dead = client.CreateReceiver(cfg.Queue, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
    var sender = client.CreateSender(cfg.Queue);

    int moved = 0;
    while (true)
    {
        var msgs = await dead.ReceiveMessagesAsync(50, TimeSpan.FromSeconds(2));
        if (msgs.Count == 0) break;

        foreach (var m in msgs)
        {
            var clone = new ServiceBusMessage(m.Body)
            {
                MessageId = m.MessageId,
                CorrelationId = m.CorrelationId,
                Subject = m.Subject,
                ContentType = m.ContentType
            };
            foreach (var kv in m.ApplicationProperties)
                clone.ApplicationProperties[kv.Key] = kv.Value;

            await sender.SendMessageAsync(clone);
            await dead.CompleteMessageAsync(m);
            moved++;
        }
    }
    return Results.Ok(new { moved });
});

app.Run();

// ===================== TYPES BELOW (AFTER TOP-LEVEL) =====================

// Config model — use regular setters (not init)
public sealed class MessagingConfig
{
    public string? ConnectionString { get; set; }
    public string? Fqdn { get; set; }              // e.g. "your-namespace.servicebus.windows.net"
    public string Queue { get; set; } = "orders";  // default
}

// DTOs / Entities
public record OrderDto(string CustomerId, decimal Amount);

public class Order
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string CustomerId { get; set; } = default!;
    public decimal Amount { get; set; }
    public DateTime CreatedUtc { get; set; } = DateTime.UtcNow;
}

public class OutboxMessage
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Type { get; set; } = default!;
    public string Payload { get; set; } = default!;
    public string? CorrelationId { get; set; }
    public DateTime CreatedUtc { get; set; } = DateTime.UtcNow;
    public DateTime? DispatchedUtc { get; set; }
    public int AttemptCount { get; set; }
}

public class ProcessedMessage
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string MessageId { get; set; } = default!;
    public DateTime ProcessedUtc { get; set; } = DateTime.UtcNow;
}

// DbContext
public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> opts) : base(opts) { }
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OutboxMessage> Outbox => Set<OutboxMessage>();
    public DbSet<ProcessedMessage> Processed => Set<ProcessedMessage>();
}

// Outbox publisher
public class OutboxPublisher : BackgroundService
{
    private readonly IServiceProvider _sp;
    private readonly ServiceBusClient _sb;
    private readonly MessagingConfig _cfg;

    private readonly IAsyncPolicy _retry = Policy
        .Handle<Exception>()
        .WaitAndRetryAsync(5, attempt =>
            TimeSpan.FromSeconds(Math.Min(30, Math.Pow(2, attempt))) +
            TimeSpan.FromMilliseconds(Random.Shared.Next(0, 250)));

    public OutboxPublisher(IServiceProvider sp, ServiceBusClient sb, IOptions<MessagingConfig> cfg)
    { _sp = sp; _sb = sb; _cfg = cfg.Value; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var sender = _sb.CreateSender(_cfg.Queue);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = _sp.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

                var batch = await db.Outbox
                    .Where(x => x.DispatchedUtc == null && x.AttemptCount < 50)
                    .OrderBy(x => x.CreatedUtc)
                    .Take(50)
                    .ToListAsync(stoppingToken);

                foreach (var msg in batch)
                {
                    await _retry.ExecuteAsync(async () =>
                    {
                        var sbMsg = new ServiceBusMessage(msg.Payload)
                        {
                            MessageId = msg.Id.ToString(),
                            Subject = msg.Type,
                            ContentType = "application/json",
                            CorrelationId = msg.CorrelationId
                        };

                        await sender.SendMessageAsync(sbMsg, stoppingToken);

                        msg.DispatchedUtc = DateTime.UtcNow;
                        msg.AttemptCount++;
                        await db.SaveChangesAsync(stoppingToken);
                    });
                }
            }
            catch
            {
                // log in real app
            }

            await Task.Delay(1000, stoppingToken);
        }
    }
}

// Queue consumer (idempotent)
public class OrderQueueConsumer : BackgroundService
{
    private readonly ServiceBusClient _sb;
    private readonly IServiceProvider _sp;
    private readonly MessagingConfig _cfg;

    public OrderQueueConsumer(ServiceBusClient sb, IServiceProvider sp, IOptions<MessagingConfig> cfg)
    { _sb = sb; _sp = sp; _cfg = cfg.Value; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var processor = _sb.CreateProcessor(_cfg.Queue, new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false,
            MaxConcurrentCalls = 8,
            PrefetchCount = 50
        });

        processor.ProcessMessageAsync += HandleAsync;
        processor.ProcessErrorAsync += err =>
        {
            // log err.Exception
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync(stoppingToken);

        stoppingToken.Register(async () =>
        {
            await processor.StopProcessingAsync();
            await processor.DisposeAsync();
        });
    }

    private async Task HandleAsync(ProcessMessageEventArgs args)
    {
        using var scope = _sp.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        var msgId = args.Message.MessageId;
        if (await db.Processed.AnyAsync(p => p.MessageId == msgId))
        {
            await args.CompleteMessageAsync(args.Message);
            return;
        }

        try
        {
            switch (args.Message.Subject)
            {
                case "OrderCreated":
                    var model = JsonSerializer.Deserialize<JsonElement>(args.Message.Body);
                    var orderId = model.GetProperty("id").GetGuid();
                    var existing = await db.Orders.FindAsync(orderId);
                    if (existing is null)
                    {
                        await db.Orders.AddAsync(new Order
                        {
                            Id = orderId,
                            CustomerId = model.GetProperty("customerId").GetString()!,
                            Amount = model.GetProperty("amount").GetDecimal(),
                            CreatedUtc = DateTime.UtcNow
                        });
                        await db.SaveChangesAsync();
                    }
                    break;
                default:
                    // unknown types -> consider DeadLetter
                    break;
            }

            db.Processed.Add(new ProcessedMessage { MessageId = msgId });
            await db.SaveChangesAsync();

            await args.CompleteMessageAsync(args.Message);
        }
        catch
        {
            await args.AbandonMessageAsync(args.Message);
            // Or DeadLetter with reason: await args.DeadLetterMessageAsync(args.Message, "processing-failed", ex.Message);
        }
    }
}
