using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using System;
using System.Text;
using System.Threading.Tasks;

namespace AdvServiceBus.DeferMessageDemo
{
    class Program
    {
        private static string QUEUE_NAME = "deferral-queue";
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
                Console.WriteLine("1: Send Ready Message");
                Console.WriteLine("2: Send Not Ready Message");
                Console.WriteLine("3: Receive Message");
                Console.WriteLine("4: Receive Deferred Message");
                Console.WriteLine("5: Clean Message");
                Console.WriteLine("0: Exit");

                var key = Console.ReadKey();

                switch (key.Key)
                {
                    case ConsoleKey.D0:
                        return;

                    case ConsoleKey.D1:
                        await SendMessage(connectionString, "We're ready for this message", true);
                        break;

                    case ConsoleKey.D2:
                        await SendMessage(connectionString, "We're not completely ready for this message just yet", false);
                        break;

                    case ConsoleKey.D3:
                        await ReceiveAndDefer(connectionString);
                        break;

                    case ConsoleKey.D4:
                        await ReceiveDefered(connectionString);
                        break;

                    case ConsoleKey.D5:
                        await ClearDeferredMessages(connectionString);
                        break;

                }

            }
        }

        private static async Task ReceiveDefered(string connectionString)
        {
            await using var serviceBusClient = new ServiceBusClient(connectionString);
            var options = new ServiceBusReceiverOptions()
            {
                ReceiveMode = ServiceBusReceiveMode.PeekLock
            };
            var messageReceiver = serviceBusClient.CreateReceiver(QUEUE_NAME, options);
            var message = await messageReceiver.ReceiveDeferredMessageAsync(_sequenceNumber);

            string messageBody = Encoding.UTF8.GetString(message.Body);

            Console.WriteLine("Message received: {0}", messageBody);

            await messageReceiver.CompleteMessageAsync(message);
        }

        private static async Task ReceiveAndDefer(string connectionString)
        {
            await using var serviceBusClient = new ServiceBusClient(connectionString);
            var options = new ServiceBusReceiverOptions()
            {
                ReceiveMode = ServiceBusReceiveMode.PeekLock
            };
            var messageReceiver = serviceBusClient.CreateReceiver(QUEUE_NAME, options);
            var message = await messageReceiver.ReceiveMessageAsync();
            if (message == null) return;

            if (message.ApplicationProperties.ContainsKey("IsReady") && !((bool)message.ApplicationProperties["IsReady"]))
            {
                _sequenceNumber = message.SequenceNumber;
                await messageReceiver.DeferMessageAsync(message);
                return;
            }

            string messageBody = Encoding.UTF8.GetString(message.Body);
            Console.WriteLine("Message received: {0}", messageBody);
            await messageReceiver.CompleteMessageAsync(message);
        }

        private static async Task SendMessage(string connectionString, string messageText, bool isReady)
        {
            await using var serviceBusClient = new ServiceBusClient(connectionString);
            var sender = serviceBusClient.CreateSender(QUEUE_NAME);

            string messageBody = $"{DateTime.Now}: {messageText} ({Guid.NewGuid()})";
            var message = new ServiceBusMessage(Encoding.UTF8.GetBytes(messageBody));
            message.ApplicationProperties.Add("IsReady", isReady);

            await sender.SendMessageAsync(message);            
        }

        private static async Task ClearDeferredMessages(string connectionString)
        {
            await using var serviceBusClient = new ServiceBusClient(connectionString);
            var options = new ServiceBusReceiverOptions()
            {
                ReceiveMode = ServiceBusReceiveMode.PeekLock
            };
            var messageReceiver = serviceBusClient.CreateReceiver(QUEUE_NAME, options);

            Console.WriteLine("Sequence Number: ");
            string sequenceNum = Console.ReadLine();

            long seqNum = long.Parse(sequenceNum);

            var msg = await messageReceiver.ReceiveDeferredMessageAsync(seqNum);

            Console.WriteLine(msg.MessageId);

            await messageReceiver.CompleteMessageAsync(msg);
        }

    }
}
