using static System.Console;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using Microsoft.Spark.Sql.Types;
using System.Collections.Generic;

namespace DataFrameApiDemo1
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder()
                                   .AppName("AuthorsAges")
                                   .GetOrCreate();

            var mySchema = new StructType(new[]
            {
                new StructField("Name", new StringType()),
                new StructField("Age", new IntegerType())
            });
            var myData = new List<GenericRow>
            {
                new GenericRow(new object[]{"Brooke", 20 }),
                new GenericRow(new object[]{ "Denny", 31 }),
                new GenericRow(new object[]{ "Jules", 30 }),
                new GenericRow(new object[]{ "TD", 35 }),
                new GenericRow(new object[]{ "Brooke", 25 }),

            };

            var data_df = spark.CreateDataFrame(myData, mySchema);
            data_df.Show();

            //Group the same names together, aggregate their ages, and compute an average
            var avg_df = data_df.GroupBy("Name").Agg(Avg("Age"));
            avg_df.Show();

            
            spark.Stop();
        }
    }
}
