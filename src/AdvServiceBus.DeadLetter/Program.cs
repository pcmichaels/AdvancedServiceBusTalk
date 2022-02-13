using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using System;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

namespace AdvServiceBus.DeadLetter
{
    class Program
    {
        private static string QUEUE_NAME = "dead-letter-demo";
        private static string DEAD_LETTER_PATH = "$deadletterqueue";
        
        static string FormatDeadLetterPath() =>
            $"{QUEUE_NAME}/{DEAD_LETTER_PATH}";

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
                Console.WriteLine("1: Send Message");
                Console.WriteLine("2: Abandon Message (Dead Letters After Max Retries)");
                Console.WriteLine("3: Dead Letter Message");
                Console.WriteLine("4: View Dead Letter Message");
                Console.WriteLine("5: Resubmit Dead Letter");
                Console.WriteLine("0: Exit");

                var key = Console.ReadKey();

                switch (key.Key)
                {
                    case ConsoleKey.D0:
                        return;

                    case ConsoleKey.D1:
                        await SendMessage(connectionString, "This message is a very bad message!");
                        break;

                    case ConsoleKey.D2:
                        await ReadMessage(connectionString, true);
                        break;

                    case ConsoleKey.D3:
                        await DeadLetterMessage(connectionString);
                        break;

                    case ConsoleKey.D4:
                        await ReadDeadLetterMessage(connectionString);
                        break;

                    case ConsoleKey.D5:
                        await ResubmitDeadLetter(connectionString);
                        break;

                }
            }
        }

        // https://stackoverflow.com/questions/41798643/resubmitting-a-message-from-dead-letter-queue-azure-service-bus
        // https://github.com/Azure/azure-sdk-for-net/issues/14038 (send via - for transaction)
        private static async Task ResubmitDeadLetter(string connectionString)
        {
            var serviceBusClient = new ServiceBusClient(connectionString);
            
            var deadLetterReceiver = serviceBusClient.CreateReceiver(FormatDeadLetterPath());
            var sender = serviceBusClient.CreateSender(QUEUE_NAME);

            var deadLetterMessage = await deadLetterReceiver.ReceiveMessageAsync();            

            using var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

            var resubmitMessage = new ServiceBusMessage(deadLetterMessage);
            await sender.SendMessageAsync(resubmitMessage);
            //throw new Exception("aa"); // - to prove the transaction
            await deadLetterReceiver.CompleteMessageAsync(deadLetterMessage);

            scope.Complete();            
        }

        private static async Task DeadLetterMessage(string connectionString)
        {
            var serviceBusClient = new ServiceBusClient(connectionString);
            var messageReceiver = serviceBusClient.CreateReceiver(QUEUE_NAME);
            var message = await messageReceiver.ReceiveMessageAsync();

            string messageBody = Encoding.UTF8.GetString(message.Body);

            await messageReceiver.DeadLetterMessageAsync(message, "Really bad message");
        }

        private static async Task ReadDeadLetterMessage(string connectionString)
        {
            var serviceBusClient = new ServiceBusClient(connectionString);
            var deadLetterReceiver = serviceBusClient.CreateReceiver(FormatDeadLetterPath());
            
            var message = await deadLetterReceiver.ReceiveMessageAsync();

            string messageBody = Encoding.UTF8.GetString(message.Body);

            Console.WriteLine("Message received: {0}", messageBody);

            // Previous versions had these as properties
            // https://www.pmichaels.net/2021/01/23/read-the-dead-letter-queue/
            if (!string.IsNullOrWhiteSpace(message.DeadLetterReason))
            {
                Console.WriteLine("Reason: {0} ", message.DeadLetterReason);
            }
            if (!string.IsNullOrWhiteSpace(message.DeadLetterErrorDescription))
            {
                Console.WriteLine("Description: {0} ", message.DeadLetterErrorDescription);
            }

            Console.WriteLine($"Message {message.MessageId} ({messageBody}) had a delivery count of {message.DeliveryCount}");
        }

        private static async Task SendMessage(string connectionString, string messageText)
        {
            var serviceBusClient = new ServiceBusClient(connectionString);
            var sender = serviceBusClient.CreateSender(QUEUE_NAME);

            string messageBody = $"{DateTime.Now}: {messageText} ({Guid.NewGuid()})";
            var message = new ServiceBusMessage(Encoding.UTF8.GetBytes(messageBody));            

            await sender.SendMessageAsync(message);
            await sender.CloseAsync();
        }

        private static async Task ReadMessage(string connectionString, bool deadLetter)
        {
            var serviceBusClient = new ServiceBusClient(connectionString);            
            var messageReceiver = serviceBusClient.CreateReceiver(QUEUE_NAME);
            var message = await messageReceiver.ReceiveMessageAsync();

            string messageBody = Encoding.UTF8.GetString(message.Body);

            if (deadLetter)
            {
                Console.WriteLine($"Message {message.MessageId} ({messageBody}) had a delivery count of {message.DeliveryCount}");
                await messageReceiver.AbandonMessageAsync(message);
            }
            else
            {
                await messageReceiver.CompleteMessageAsync(message);
            }            

            Console.WriteLine("Message received: {0}", messageBody);
        }    
    }
}
