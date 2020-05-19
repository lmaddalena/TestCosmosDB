using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Net;
using Microsoft.Azure.Cosmos;

namespace TestCosmosDB
{
    class Program
    {
        // the CosmosDB account endpoint URI
        private static string endpoint = "<endpoint uri>";

        // the CosmosDB account "primary" or "secondary" key
        private static string key = "<key>";
        static async Task Main(string[] args)
        {
            try
            {
                System.Console.WriteLine("Start demo....");
                Program p = new Program();
                await p.Start();
            }
            catch (CosmosException ce)
            {
                System.Console.WriteLine($"{ce.StatusCode} errore occurred:{ce.Message}");
 
            }
            catch(Exception ex)
            {
                System.Console.WriteLine("Error " + ex);
            }
            finally
            {
                System.Console.WriteLine("Press [ENTER] to quit.");
                System.Console.ReadLine();
            }
        }

        private async Task Start()
        {
            string databaseName = "FamilyDB";
            string containerName = "FamilyContainer";
            string partitionKey = "/LastName";


            CosmosClient cli = new CosmosClient(endpoint, key);            
            Database db = await this.CreateDatabaseAsync(cli, databaseName);
            Container cont = await this.CreateContainerAsync(db, containerName, partitionKey);
            await this.AddSomeFamiliesToContainer(cont);
            await this.QueryItemsAsync(cont);
            await this.DeleteDatabaseAsync(db);

            // cleaup
            cli.Dispose();

        }

        private async Task<Database> CreateDatabaseAsync(CosmosClient client, string databaseName)
        {
            System.Console.WriteLine("Creating database...");
            Database db = await client.CreateDatabaseIfNotExistsAsync(databaseName);
            return db;
        }

        private async Task<Container> CreateContainerAsync(Database database, string containerName, string partitionKey)
        {
            System.Console.WriteLine("Creating container...");
            Container cont = await database.CreateContainerIfNotExistsAsync(containerName, partitionKey);
            return cont;
        }

        private async Task AddItemToContainer(Container container, Family item)
        {
            System.Console.WriteLine("Add item...");


            // check if exists
            try
            {
                // Cosmos DB throw an exception if item doesen't exist
                ItemResponse<Family> res = await container.ReadItemAsync<Family>(item.Id, new PartitionKey(item.LastName));
                System.Console.WriteLine($"Item with id {item.Id} already exists.");
            }
            catch (CosmosException ce)
            {
                if(ce.StatusCode == HttpStatusCode.NotFound)
                {
                    ItemResponse<Family> res = await container.CreateItemAsync<Family>(item, new PartitionKey(item.LastName));

                    // Note that after creating the item, we can access the body of the item with the Resource property off the ItemResponse. We can also access the RequestCharge property to see the amount of RUs consumed on this request.
                    Console.WriteLine("Created item in database with id: {0} Operation consumed {1} RUs.\n", res.Resource.Id, res.RequestCharge);
                    
                }
                else
                {
                    throw;
                }
            }
            catch (System.Exception)
            {
                
                throw;
            }
            
        }

        private async Task DeleteDatabaseAsync(Database database)
        {
            System.Console.WriteLine("Delete database..");
            DatabaseResponse databaseResourceResponse = await database.DeleteAsync();
        }
        private async Task AddSomeFamiliesToContainer(Container container)
        {
            Family andersenFamily = new Family
            {
                Id = "Andersen.1",
                LastName = "Andersen",
                Parents = new Parent[]
                {
                    new Parent { FirstName = "Thomas" },
                    new Parent { FirstName = "Mary Kay" }
                },
                Children = new Child[]
                {
                    new Child
                    {
                        FirstName = "Henriette Thaulow",
                        Gender = "female",
                        Grade = 5,
                        Pets = new Pet[]
                        {
                            new Pet { GivenName = "Fluffy" }
                        }
                    }
                },
                Address = new Address { State = "WA", County = "King", City = "Seattle" },
                IsRegistered = false
            };

            Family wakefieldFamily = new Family
            {
                Id = "Wakefield.7",
                LastName = "Wakefield",
                Parents = new Parent[]
                {
                    new Parent { FamilyName = "Wakefield", FirstName = "Robin" },
                    new Parent { FamilyName = "Miller", FirstName = "Ben" }
                },
                Children = new Child[]
                {
                    new Child
                    {
                        FamilyName = "Merriam",
                        FirstName = "Jesse",
                        Gender = "female",
                        Grade = 8,
                        Pets = new Pet[]
                        {
                            new Pet { GivenName = "Goofy" },
                            new Pet { GivenName = "Shadow" }
                        }
                    },
                    new Child
                    {
                        FamilyName = "Miller",
                        FirstName = "Lisa",
                        Gender = "female",
                        Grade = 1
                    }
                },
                Address = new Address { State = "NY", County = "Manhattan", City = "NY" },
                IsRegistered = true
            };

            await this.AddItemToContainer(container, andersenFamily);
            await this.AddItemToContainer(container, wakefieldFamily);
        }

        private async Task QueryItemsAsync(Container container)
        {
            var sqlQueryText = "SELECT * FROM c WHERE c.LastName = 'Andersen'";

            Console.WriteLine("Running query: {0}\n", sqlQueryText);

            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            FeedIterator<Family> queryResultSetIterator = container.GetItemQueryIterator<Family>(queryDefinition);

            List<Family> families = new List<Family>();

            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<Family> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                foreach (Family family in currentResultSet)
                {
                    families.Add(family);
                    Console.WriteLine("\tRead {0}\n", family);
                }
            }            
        }
    }
}
