using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Configuration;
using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AdvServiceBus.ServiceBusManagement
{
    class Program
    {
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
                Console.WriteLine("1: Delete queues");
                Console.WriteLine("2: Create queues");                
                Console.WriteLine("3: Show connection details");
                Console.WriteLine("4: Send message to source queue");
                Console.WriteLine("5: Set-up Auto-Delete Queue");
                Console.WriteLine("6: Send Messages to Auto Delete");
                Console.WriteLine("0: Exit");

                var serviceBusAdministrationClient = new ServiceBusAdministrationClient(connectionString);

                var key = Console.ReadKey();

                switch (key.Key)
                {
                    case ConsoleKey.D0:
                        return;

                    case ConsoleKey.D1:
                        await RemoveQueue(connectionString, "auto-forward-queue-source");
                        await RemoveQueue(connectionString, "auto-forward-queue-destination");
                        break;

                    case ConsoleKey.D2:
                        await CreateAutoForwardQueues(connectionString, "auto-forward-queue-source", "auto-forward-queue-destination");
                        break;

                    case ConsoleKey.D3:
                        await ShowConnectionDetails(connectionString, "auto-forward-queue-source");
                        break;

                    case ConsoleKey.D4:
                        await SendMessage(connectionString, "Seven or eleven, snake eyes watching you",
                            "auto-forward-queue-source");
                        break;

                    case ConsoleKey.D5:
                        await CreateAutoDeleteQueue(connectionString, "auto-delete-queue");
                        break;

                    case ConsoleKey.D6:
                        for (int i = 0; i <= 100; i++)
                        {
                            await SendMessage(connectionString, "This message is no longer useful",
                                "auto-delete-queue");
                        }
                        break;

                }
            }
        }

        private static async Task ShowConnectionDetails(string connectionString, string queueName)
        {
            var serviceBusAdministrationClient = new ServiceBusAdministrationClient(connectionString);
            var queue = await serviceBusAdministrationClient.GetQueueAsync(queueName);
            
            foreach (var authRules in queue.Value.AuthorizationRules)
            {
                Console.WriteLine($"Key Name: {authRules.KeyName} / Rights: {string.Join(",", authRules.Rights.ToList())}");
                Console.WriteLine($"Primary Key: {((SharedAccessAuthorizationRule)authRules).PrimaryKey}");
            }
        }

        private static async Task SendMessage(string connectionString, string messageText, string queueName)
        {
            var queueClient = new QueueClient(connectionString, queueName);

            string messageBody = $"{DateTime.Now}: {messageText} ({Guid.NewGuid()})";
            var message = new Message(Encoding.UTF8.GetBytes(messageBody));            

            await queueClient.SendAsync(message);
            await queueClient.CloseAsync();
        }

        private static async Task RemoveQueue(string connectionString, string queueName)
        {
            var serviceBusAdministrationClient = new ServiceBusAdministrationClient(connectionString);
            await serviceBusAdministrationClient.DeleteQueueAsync(queueName);
        }

        private static async Task CreateAutoForwardQueues(string connectionString, string source, string destination)
        {
            // Create authorisation rules
            var authorisationRule = new SharedAccessAuthorizationRule(
                "manage", new[] { AccessRights.Manage, AccessRights.Listen, AccessRights.Send });

            var serviceBusAdministrationClient = new ServiceBusAdministrationClient(connectionString);

            // Create auto-forward queues
            var optionsDest = new CreateQueueOptions(destination);
            optionsDest.AuthorizationRules.Add(authorisationRule);
            var queueDest = await serviceBusAdministrationClient.CreateQueueAsync(optionsDest);

            var options = new CreateQueueOptions(source)
            {
                ForwardTo = destination                          
            };
            options.AuthorizationRules.Add(authorisationRule);
            
            var queue = await serviceBusAdministrationClient.CreateQueueAsync(options);

        }

        private static async Task CreateAutoDeleteQueue(string connectionString, string queueName)
        {
            // Create authorisation rules
            var authorisationRule = new SharedAccessAuthorizationRule(
                "manage", new[] { AccessRights.Manage, AccessRights.Listen, AccessRights.Send });

            var serviceBusAdministrationClient = new ServiceBusAdministrationClient(connectionString);
                
            var options = new CreateQueueOptions(queueName)
            {
                AutoDeleteOnIdle = TimeSpan.FromMinutes(5)
            };
            options.AuthorizationRules.Add(authorisationRule);

            var queue = await serviceBusAdministrationClient.CreateQueueAsync(options);

        }

    }
}
