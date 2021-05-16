using static System.Console;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace WordCount
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder()
                                    .AppName("wordcount")
                                    .GetOrCreate();

            var mnm_df = spark.Read()
                        .Format("csv")
                        .Option("header", "true")
                        .Option("inferSchema", "true")
                        .Load("mnm_dataset.csv");
            mnm_df.Show();
            var count_mnm_df = mnm_df.Select("State", "Color", "Count")
                                    .GroupBy("State", "Color")
                                    .Agg(Count("Count")).ToDF("State", "Color", "Total")
                                    .OrderBy("Total");

            count_mnm_df.Show(60);
            WriteLine($"total rows = {count_mnm_df.Count()}");

            //just for CA state
            var ca_count_mnm_df = mnm_df.Select("State", "Color", "Count")
                                    .Where("State='CA'")
                                    .GroupBy("State", "Color")
                                    .Agg(Count("Count")).ToDF("State", "Color", "Total")
                                    .OrderBy("Total");

            ca_count_mnm_df.Show(20);

            spark.Stop();
        }
    }
}
