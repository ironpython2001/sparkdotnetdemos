using System;
using System.Linq;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace mySparkApp
{
    class Program
    {
        static void Main(string[] args)
        {
           // Create a Spark session
            SparkSession spark = SparkSession
                .Builder()
                .AppName("word_count_sample")
                .GetOrCreate();

            // Create initial DataFrame
            DataFrame df1 = spark.Read().Text("input.txt");

            //words
            var df2 = df1.Select(Functions.Split(Functions.Col("value"), " ").Alias("words"));

            //word
            var df3 = df2.Select(Functions.Explode(Functions.Col("words")).Alias("word"));

            var rs = df3.GroupBy("word").Count();

            var s = rs.OrderBy(Functions.Col("count").Desc());
            s.Show();

            // Count words
            DataFrame words = df1
                .Select(Functions.Split(Functions.Col("value"), " ").Alias("words"))
                .Select(Functions.Explode(Functions.Col("words")).Alias("word"))
                .GroupBy("word")
                .Count()
                .OrderBy(Functions.Col("count").Desc());

            // Show results
            words.Show();

            // Stop Spark session
            spark.Stop();
        }
    }
}
