using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using System;
using System.Text;
using System.Threading.Tasks;

namespace AdvServiceBus.QueueSessionDemo
{
    class Program
    {
        private static string QUEUE_NAME = "queue-session-demo";

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
                Console.WriteLine("1: Send Messages");
                Console.WriteLine("2: Receive Messages session1");
                Console.WriteLine("3: Send and await reply");
                Console.WriteLine("4: Receive Messages session1Send");
                Console.WriteLine("5: Reply");
                Console.WriteLine("0: Exit");

                var key = Console.ReadKey();

                switch (key.Key)
                {
                    case ConsoleKey.D0:
                        return;

                    case ConsoleKey.D1:
                        await SendMessage(connectionString, "Message", "session1");
                        await SendMessage(connectionString, "NoiseMessage", "session2");
                        break;

                    case ConsoleKey.D2:
                        await ReadMessage(connectionString, "session1");
                        break;

                    case ConsoleKey.D3:
                        await SendMessage(connectionString, "Ticking away the moments that make up a dull day", "session1Send");
                        await ReadMessage(connectionString, "session2Reply");
                        break;

                    case ConsoleKey.D4:
                        await ReadMessage(connectionString, "session1Send");
                        break;

                    case ConsoleKey.D5:
                        await SendMessage(connectionString, "Time, Pink Floyd", "session2Reply");
                        break;

                }
            }
        }

        private static async Task SendMessage(string connectionString, string messageText, string sessionId)
        {
            await using var serviceBusClient = new ServiceBusClient(connectionString);
            var sender = serviceBusClient.CreateSender(QUEUE_NAME);

            string messageBody = $"{DateTime.Now}: {messageText} ({Guid.NewGuid()})";
            var message = new ServiceBusMessage(Encoding.UTF8.GetBytes(messageBody));
            message.SessionId = sessionId;

            await sender.SendMessageAsync(message);
        }

        private static async Task ReadMessage(string connectionString, string sessionId)
        {
            await using var serviceBusClient = new ServiceBusClient(connectionString);            
            var session = await serviceBusClient.AcceptSessionAsync(QUEUE_NAME, sessionId);

            var message = await session.ReceiveMessageAsync();
            await session.CompleteMessageAsync(message);

            string messageBody = Encoding.UTF8.GetString(message.Body);

            Console.WriteLine("Message received: {0}", messageBody);
        }

    }
}
