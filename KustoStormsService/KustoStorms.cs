using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;

namespace Kusto.Storms
{
    public class KustoStorms
    {
        private string _kustoConnectionString;
        private string _databaseName;

        public KustoStorms(string kustoConnectionString, string databaseName)
        {
            _kustoConnectionString = kustoConnectionString;
            _databaseName = databaseName;
        }

        public long CountRows()
        {
            long numberOfStorms = 0;
            var kcsb = new KustoConnectionStringBuilder(_kustoConnectionString, _databaseName);            
            using (var queryProvider = KustoClientFactory.CreateCslQueryProvider(kcsb))
            {
                // The query -- Note that for demonstration purposes, we send a query that asks for two different
                // result sets (HowManyRecords and SampleRecords).
                var query = "MyIngestedSample | count | as HowManyRecords";

                // It is strongly recommended that each request has its own unique
                // request identifier. This is mandatory for some scenarios (such as cancelling queries)
                // and will make troubleshooting easier in others.
                var clientRequestProperties = new ClientRequestProperties() { ClientRequestId = Guid.NewGuid().ToString() };
                using (var reader = queryProvider.ExecuteQuery(query, clientRequestProperties))
                {
                    // Read HowManyRecords
                    while (reader.Read())
                    {
                        numberOfStorms = reader.GetInt64(0);
                        Console.WriteLine($"There are {numberOfStorms} records in the table");
                    }
                }
            }
            return numberOfStorms;
        }
    }
}
