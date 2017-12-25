using System;
using StackExchange.Redis;
namespace redisClient
{
    
    class Program
    {
        static string inProcess = "inprocess:jobs";
        static string rServer = "192.168.1.18";
        static ConnectionMultiplexer _redis = ConnectionMultiplexer.Connect(rServer);
        static IDatabase _db = _redis.GetDatabase();

        static void DoWork(string jobId)
        {
            // Store the work item on the in_process queue
            _db.ListLeftPush(inProcess, jobId);

            // Process the job
            Console.WriteLine(jobId);

            // Remove it from the in_process queue
            _db.ListRightPop(inProcess);
        }
 
        static void Main(string[] args)
        {
            string topic = "work_to_do";
            
            // Subscripe to the "Work Is Ready" topic.  
            // Check with the publisher.
            ISubscriber sub = _redis.GetSubscriber();
            
            sub.Subscribe(topic, (channel, queue) =>
            {
                // The message in this topic is the name of the 
                // queue on which the job id was placed.
                Console.WriteLine((string)queue);

                // Grab the work message from the queue
                Console.WriteLine("popping");
                string work = _db.ListRightPop((string)queue);

                // do it
                if (work != null)
                    DoWork(work); 
            });

            // chill out until some work needs to be done.
            while (true) ;
        }
    }
}
