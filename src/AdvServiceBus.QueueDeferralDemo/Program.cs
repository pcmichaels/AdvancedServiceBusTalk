using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AdvServiceBus.QueueDeferralDemo
{
    class Program
    {
        private static string QUEUE_NAME = "deferral-queue";

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
                Console.WriteLine("1: Receive Messages (Events)");
                Console.WriteLine("2: Send Messages");
                Console.WriteLine("0: Exit");

                var key = Console.ReadKey();

                switch (key.Key)
                {
                    case ConsoleKey.D0:
                        return;

                    case ConsoleKey.D1:
                        await ReadMessageEvent(connectionString);
                        break;

                    case ConsoleKey.D2:
                        await SendScheduledMessage(connectionString, DateTime.UtcNow.AddSeconds(10));
                        break;

                }

            }
        }

        private static async Task ReadMessageEvent(string connectionString)
        {
            await using var serviceBusClient = new ServiceBusClient(connectionString);
            var processor = serviceBusClient.CreateProcessor(QUEUE_NAME);
            processor.ProcessMessageAsync += handleMessage;
            processor.ProcessErrorAsync += ExceptionHandler;
            await processor.StartProcessingAsync();
        }

        private static Task ExceptionHandler(ProcessErrorEventArgs arg)
        {
            Console.WriteLine("Something bad happened!");
            return Task.CompletedTask;
        }

        private static Task handleMessage(ProcessMessageEventArgs arg)
        {
            string messageBody = Encoding.UTF8.GetString(arg.Message.Body);
            Console.WriteLine("Message received: {0}", messageBody);

            return Task.CompletedTask;
        }

        private static async Task SendScheduledMessage(string connectionString, DateTime dateTime)
        {
            await using var serviceBusClient = new ServiceBusClient(connectionString);
            var sender = serviceBusClient.CreateSender(QUEUE_NAME);

            string messageBody = $"{DateTime.Now}: Hello Everybody! ({Guid.NewGuid()}) You won't get this until {dateTime}";
            var message = new ServiceBusMessage(Encoding.UTF8.GetBytes(messageBody));

            long sequenceNumber = await sender.ScheduleMessageAsync(message, dateTime);
            //await sender.CancelScheduledMessageAsync(sequenceNumber);

        }
    }
}
