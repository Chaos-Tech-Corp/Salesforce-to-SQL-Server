using System;

namespace Salesforce_to_SQLServer
{
    class Program
    {
        static void Main(string[] args)
        {

            var sf = new SFSync("salesforce:username", "salesforce:password", "salesforce:token", "salesforce:application:key", "salesforce:application:secret");

            sf.Connect("connection:string", "databaseName");
            sf.SetupDatabase();
            sf.Start();

        }
    }
}
