using static System.Console;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace SparkSqlOperations
{
    class Program
    {
        static void Main(string[] args)
        {

            var spark1 = SparkSession.Builder()
                                    .AppName("wordcount")
                                    .GetOrCreate();

            var mnm_df = spark1.Read()
                       .Format("csv")
                       .Option("header", "true")
                       .Option("inferSchema", "true")
                       .Load("mnm_dataset.csv");

            //page no 78
            var count_mnm_df = mnm_df
                                .Select("State", "Color", "Count")
                                .GroupBy("State", "Color")
                                .Agg(Count("Count")
                                .Alias("Total"))
                                .OrderBy(Asc("Total"));

            count_mnm_df.Explain(true);


            spark1.Stop();

            //page 85
            var spark2 = SparkSession
                         .Builder()
                         .AppName("SparkSQLExampleApp")
                         .GetOrCreate();

            
            var df = spark2.Read().Format("csv")
                            .Option("inferSchema", "true")
                            .Option("header", "true")
                            .Load("departuredelays.csv");

            //Read and create a temporary view
            //Infer schema (note that for larger files you
            //may want to specify the schema)

            df.CreateOrReplaceTempView("us_delay_flights_tbl");

            //page 86
            //use temp view to query 
            spark2.Sql(@"SELECT distance, origin, destination
FROM us_delay_flights_tbl WHERE distance > 1000
ORDER BY distance DESC").Show();

            //page 87
            spark2.Sql(@"SELECT date, delay, origin, destination
FROM us_delay_flights_tbl
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
ORDER by delay DESC").Show();


            spark2.Stop();

        }
    }
}
