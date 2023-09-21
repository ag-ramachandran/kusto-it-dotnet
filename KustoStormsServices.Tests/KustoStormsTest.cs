using NUnit.Framework;
using Kusto.Storms;
using Testcontainers.Kusto;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;

namespace KustoStormsServices.Tests
{
    [TestFixture]
    public class CountStorms_Should_Count_Storms_By_State
    {
        private KustoStorms _kustoStorms;
        private KustoContainer _kustoContainer;

        [SetUp]
        public void SetUp()
        {

            _kustoContainer = new KustoBuilder().WithHostname("sdktest-local.westeurope.dev.kusto.windows.net").WithName("sdktest-local.westeurope.dev.kusto.windows.net").Build();
            _kustoContainer.StartAsync().Wait();
            _kustoStorms = new KustoStorms(_kustoContainer.GetConnectionString(), "NetDefaultDB");
            var kcs = _kustoContainer.GetConnectionString();
            var kcsb = new KustoConnectionStringBuilder(kcs, "NetDefaultDB");

            using (var adminQueryProvider = KustoClientFactory.CreateCslAdminProvider(kcsb))
            {
                adminQueryProvider.ExecuteControlCommand(".create table MyIngestedSample(Name:string, Id:int)");
                adminQueryProvider.ExecuteControlCommand(@".ingest inline into table MyIngestedSample <| 
                                    Alice,1000
                                    Bob,2000
                                    Charlie,3000"
                );
            }
        }

        [Test]
        public void GetCount_ShouldHave_ThreeRows()
        {

            var result = _kustoStorms.CountRows();
            Assert.That(result, Is.EqualTo(3), "There should be 3 rows (ingested and queried)");
        }

        [TearDown]
        public void CleanUp()
        {

            _kustoContainer.StopAsync();
        }

    }
}