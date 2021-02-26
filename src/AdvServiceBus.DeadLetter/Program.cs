using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Extensions.Configuration;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace AdvServiceBus.DeadLetter
{
    class Program
    {
        private static string QUEUE_NAME = "dead-letter-demo";

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
            var serviceBusConnection = new ServiceBusConnection(connectionString);

            var deadletterPath = EntityNameHelper.FormatDeadLetterPath(QUEUE_NAME);
            var deadLetterReceiver = new MessageReceiver(serviceBusConnection, deadletterPath, ReceiveMode.PeekLock);
            
            var queueClient = new QueueClient(serviceBusConnection, QUEUE_NAME, ReceiveMode.PeekLock, RetryPolicy.Default);

            var deadLetterMessage = await deadLetterReceiver.ReceiveAsync();

            using var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

            var resubmitMessage = deadLetterMessage.Clone();

            resubmitMessage.UserProperties.Remove("DeadLetterReason");
            resubmitMessage.UserProperties.Remove("DeadLetterErrorDescription");
            
            await queueClient.SendAsync(resubmitMessage);
            //throw new Exception("aa"); // - to prove the transaction
            await deadLetterReceiver.CompleteAsync(deadLetterMessage.SystemProperties.LockToken);            

            scope.Complete();            
        }

        private static async Task DeadLetterMessage(string connectionString)
        {
            var messageReceiver = new MessageReceiver(connectionString, QUEUE_NAME);
            var message = await messageReceiver.ReceiveAsync();

            string messageBody = Encoding.UTF8.GetString(message.Body);

            await messageReceiver.DeadLetterAsync(message.SystemProperties.LockToken, "Really bad message");
        }

        private static async Task ReadDeadLetterMessage(string connectionString)
        {
            var deadletterPath = EntityNameHelper.FormatDeadLetterPath(QUEUE_NAME);
            var deadLetterReceiver = new MessageReceiver(connectionString, deadletterPath, ReceiveMode.PeekLock);
            
            var message = await deadLetterReceiver.ReceiveAsync();

            string messageBody = Encoding.UTF8.GetString(message.Body);

            Console.WriteLine("Message received: {0}", messageBody);
            if (message.UserProperties.ContainsKey("DeadLetterReason"))
            {
                Console.WriteLine("Reason: {0} ", message.UserProperties["DeadLetterReason"]);
            }
            if (message.UserProperties.ContainsKey("DeadLetterErrorDescription"))
            {
                Console.WriteLine("Description: {0} ", message.UserProperties["DeadLetterErrorDescription"]);
            }

            Console.WriteLine($"Message {message.MessageId} ({messageBody}) had a delivery count of {message.SystemProperties.DeliveryCount}");
        }

        private static async Task SendMessage(string connectionString, string messageText)
        {
            var queueClient = new QueueClient(connectionString, QUEUE_NAME);

            string messageBody = $"{DateTime.Now}: {messageText} ({Guid.NewGuid()})";
            var message = new Message(Encoding.UTF8.GetBytes(messageBody));            

            await queueClient.SendAsync(message);
            await queueClient.CloseAsync();
        }

        private static async Task ReadMessage(string connectionString, bool deadLetter)
        {
            var messageReceiver = new MessageReceiver(connectionString, QUEUE_NAME);
            var message = await messageReceiver.ReceiveAsync();

            string messageBody = Encoding.UTF8.GetString(message.Body);

            if (deadLetter)
            {
                Console.WriteLine($"Message {message.MessageId} ({messageBody}) had a delivery count of {message.SystemProperties.DeliveryCount}");
                await messageReceiver.AbandonAsync(message.SystemProperties.LockToken);
            }
            else
            {
                await messageReceiver.CompleteAsync(message.SystemProperties.LockToken);
            }            

            Console.WriteLine("Message received: {0}", messageBody);
        }    
    }
}
