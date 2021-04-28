using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AdvServiceBus.Performance
{
    class Program
    {
        private static string QUEUE_NAME = "batch-demo";

        static async Task Main(string[] args)
        {
            IConfiguration configuration = new ConfigurationBuilder()
               .AddJsonFile("appsettings.json", true, true)
               .AddUserSecrets<Program>()
               .Build();

            string connectionString = configuration.GetValue<string>("ServiceBusConnectionString");

            while (true)
            {
                Console.WriteLine("Choose Action:");
                Console.WriteLine("1: Send 1000 Messages");
                Console.WriteLine("2: Send 1000 Messages Batched");
                Console.WriteLine("3: Receive 1000 Messages in Queue");
                Console.WriteLine("4: Batch Receive 1000 Messages in Queue");
                Console.WriteLine("5: Prefetch Receive 1000 Messages in Queue");
                Console.WriteLine("6: Prefetch Batch Receive 1000 Messages in Queue");
                Console.WriteLine("0: Exit");

                var key = Console.ReadKey();

                switch (key.Key)
                {
                    case ConsoleKey.D0:
                        return;

                    case ConsoleKey.D1:
                        await SendMessages(connectionString, "batch test", 1000);
                        break;

                    case ConsoleKey.D2:
                        await SendMessagesBatch(connectionString, "batch test", 1000);
                        break;

                    case ConsoleKey.D3:
                        await ReceiveMessages(connectionString, 1000);
                        break;

                    case ConsoleKey.D4:
                        await ReceiveMessagesBatch(connectionString, 1000);
                        break;

                    case ConsoleKey.D5:
                        await ReceiveMessagesPrefetch(connectionString, 1000, 150);
                        break;

                    case ConsoleKey.D6:
                        await ReceiveMessagesBatchPrefetch(connectionString, 1000, 100, 30);
                        break;

                }
            }
        }

        private static async Task SendMessages(string connectionString, string messageText, int messageCount)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var queueClient = new QueueClient(connectionString, QUEUE_NAME);

            for (int i = 0; i < messageCount; i++)
            {
                string messageBody = $"{DateTime.Now}: {messageText} ({Guid.NewGuid()})";
                var message = new Message(Encoding.UTF8.GetBytes(messageBody));

                await queueClient.SendAsync(message);
            }
            await queueClient.CloseAsync();

            stopwatch.Stop();
            Console.WriteLine($"Send messages took {stopwatch.ElapsedMilliseconds}");
        }

        private static async Task SendMessagesBatch(string connectionString, string messageText, int messageCount)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var queueClient = new QueueClient(connectionString, QUEUE_NAME);
            var messages = new List<Message>();

            for (int i = 0; i < messageCount; i++)
            {
                string messageBody = $"{DateTime.Now}: {messageText} ({Guid.NewGuid()})";
                var message = new Message(Encoding.UTF8.GetBytes(messageBody));                

                messages.Add(message);
            }
            await queueClient.SendAsync(messages);
            await queueClient.CloseAsync();

            stopwatch.Stop();
            Console.WriteLine($"Send messages took {stopwatch.ElapsedMilliseconds}");
        }

        private static async Task ReceiveMessages(string connectionString, int count)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var messageReceiver = new MessageReceiver(connectionString, QUEUE_NAME);
            for (int i = 0; i < count; i++)
            {
                var message = await messageReceiver.ReceiveAsync();
                string messageBody = Encoding.UTF8.GetString(message.Body);
                Console.WriteLine($"Message received: {messageBody}");

                await messageReceiver.CompleteAsync(message.SystemProperties.LockToken);
            }

            stopwatch.Stop();
            Console.WriteLine($"Receive messages took {stopwatch.ElapsedMilliseconds}");
        }

        private static async Task ReceiveMessagesBatch(string connectionString, int count)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            int remainingCount = count;

            while (remainingCount > 0)
            {
                var messageReceiver = new MessageReceiver(connectionString, QUEUE_NAME);
                var messages = await messageReceiver.ReceiveAsync(remainingCount, TimeSpan.FromSeconds(20));

                foreach (var message in messages)
                {
                    string messageBody = Encoding.UTF8.GetString(message.Body);
                    Console.WriteLine($"Message received: {messageBody}");
                    remainingCount--;
                }

                await messageReceiver.CompleteAsync(messages.Select(a => a.SystemProperties.LockToken));
            }

            stopwatch.Stop();
            Console.WriteLine($"Receive messages took {stopwatch.ElapsedMilliseconds}");
            Console.WriteLine($"Remaining count: {remainingCount}");
        }

        private static async Task ReceiveMessagesPrefetch(string connectionString, int count, int prefetchCount)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var messageReceiver = new MessageReceiver(connectionString, QUEUE_NAME);
            messageReceiver.PrefetchCount = prefetchCount;
            for (int i = 0; i < count; i++)
            {
                var message = await messageReceiver.ReceiveAsync(TimeSpan.FromSeconds(60));
                string messageBody = Encoding.UTF8.GetString(message.Body);
                Console.WriteLine($"Message received: {messageBody}");

                await messageReceiver.CompleteAsync(message.SystemProperties.LockToken);
            }

            stopwatch.Stop();
            Console.WriteLine($"Receive messages took {stopwatch.ElapsedMilliseconds}");
        }

        private static async Task ReceiveMessagesBatchPrefetch(string connectionString, int count, int prefetchCount, int batchReceiveCount)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            int remainingCount = count;

            while (remainingCount > 0)
            {
                var messageReceiver = new MessageReceiver(connectionString, QUEUE_NAME);
                messageReceiver.PrefetchCount = prefetchCount;
                var messages = await messageReceiver.ReceiveAsync(remainingCount > batchReceiveCount ? batchReceiveCount : remainingCount);
                if (messages == null) break;

                foreach (var message in messages)
                {
                    string messageBody = Encoding.UTF8.GetString(message.Body);
                    Console.WriteLine($"Message received: {messageBody}");
                    remainingCount--;
                }

                await messageReceiver.CompleteAsync(messages.Select(a => a.SystemProperties.LockToken));
            }

            stopwatch.Stop();
            Console.WriteLine($"Receive messages took {stopwatch.ElapsedMilliseconds}");
            Console.WriteLine($"Remaining count: {remainingCount}");
        }

    }
}
