using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Extensions.Configuration;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AdvServiceBus.AutoForwardingDemo
{
    class Program
    {
        private static string QUEUE_NAME = "demo-queue";
        private static string QUEUE_NAME_FORWARDED = "demo-queue-forwarded";
        private static long _sequenceNumber;

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
                Console.WriteLine("1: Receive Messages On Both Queues");
                Console.WriteLine("2: Send Message");
                Console.WriteLine("3: Configure Forwarding");                
                Console.WriteLine("0: Exit");

                var key = Console.ReadKey();

                switch (key.Key)
                {
                    case ConsoleKey.D0:
                        return;

                    case ConsoleKey.D1:
                        await ReadMessageEvent(connectionString, QUEUE_NAME);
                        await ReadMessageEvent(connectionString, QUEUE_NAME_FORWARDED);
                        break;

                    case ConsoleKey.D2:
                        await SendMessage(connectionString, "Gunter gleiben glauchen globen", false);
                        break;

                    case ConsoleKey.D3:
                        await ConfigureForwarding(connectionString, QUEUE_NAME, QUEUE_NAME_FORWARDED);
                        break;

                    case ConsoleKey.D4:
                        await ReceiveDefered(connectionString);
                        break;

                }

            }
        }

        private static Task ConfigureForwarding(string connectionString, string queueName, string queueNameForward)
        {
            var queueClient = new QueueClient(connectionString, queueName);
            var subscriptionClient = new SubscriptionClient()
            queueClient.

        }

        private static Task ReadMessageEvent(string connectionString, string queueName)
        {
            var queueClient = new QueueClient(connectionString, queueName);

            var messageHandlerOptions = new MessageHandlerOptions(ExceptionHandler);
            queueClient.RegisterMessageHandler(handleMessage, messageHandlerOptions);

            return Task.CompletedTask;
        }

        private static Task ExceptionHandler(ExceptionReceivedEventArgs arg)
        {
            Console.WriteLine("Something bad happened!");
            return Task.CompletedTask;
        }

        private static Task handleMessage(Message message, CancellationToken cancellation)
        {
            string messageBody = Encoding.UTF8.GetString(message.Body);
            Console.WriteLine("Message received: {0}", messageBody);

            return Task.CompletedTask;
        }

        private static async Task SendMessage(string connectionString, string messageText, bool isReady)
        {
            var queueClient = new QueueClient(connectionString, QUEUE_NAME);

            string messageBody = $"{DateTime.Now}: {messageText} ({Guid.NewGuid()})";
            var message = new Message(Encoding.UTF8.GetBytes(messageBody));
            message.UserProperties.Add("IsReady", isReady);

            await queueClient.SendAsync(message);
            await queueClient.CloseAsync();
        }

    }
}
