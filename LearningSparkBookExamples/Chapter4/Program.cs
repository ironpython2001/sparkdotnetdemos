using static System.Console;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using Microsoft.Spark.Sql.Types;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Reflection;

namespace Chapter4
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark1 = SparkSession.Builder()
                                 .AppName("chapter4")
                                 .GetOrCreate();
            var csvFile = "departuredelays.csv";

            //page no 85
            // Read and create a temporary view
            // Infer schema (note that for larger files you may want to specify the schema)
            var df = spark1.Read().Format("csv")
                            .Option("inferSchema", "true")
                            .Option("header", "true")
                            .Load(csvFile);

            // Create a temporary view
            df.CreateOrReplaceTempView("us_delay_flights_tbl");

            //find all flights whose distance is greater than 1,000 miles
            spark1.Sql(@"SELECT distance, origin, destination 
                        FROM us_delay_flights_tbl WHERE distance > 1000
                        ORDER BY distance DESC").Show(10);

            // above query can also be written as follows
            df.Select("distance", "origin", "destination")
                    .Where(Col("distance") > 1000)
                .OrderBy(Desc("distance")).Show();

            //find all flights between San Francisco (SFO) and Chicago(ORD)
            spark1.Sql(@"SELECT date, delay, origin, destination
                    FROM us_delay_flights_tbl
                    WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
                    ORDER by delay DESC").Show(10);

            //CASE clause in SQL
            spark1.Sql(@"SELECT delay, origin, destination,
                             CASE
                             WHEN delay > 360 THEN 'Very Long Delays'
                             WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
                             WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
                             WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
                             WHEN delay = 0 THEN 'No Delays'
                             ELSE 'Early'
                             END AS Flight_Delays
                             FROM us_delay_flights_tbl
                             ORDER BY origin, delay DESC").Show(10);


            spark1.Stop();

            //SQL Tables and Views
            //page no 90
            
            var conf = new Microsoft.Spark.SparkConf()
                            .Set("spark.sql.warehouse.dir", "hdfs://namenode/sql/metadata/hive")
                            .Set("spark.sql.catalogImplementation", "hive")
                            .SetMaster("local[*]")
                            .SetAppName("chapter4_2");

            var spark2 = SparkSession.Builder()
                                 .Config(conf)
                                .EnableHiveSupport()
                                .GetOrCreate();

            

            //Creating SQL Databases and Tables
            spark2.Sql("CREATE DATABASE learn_spark_db");
            spark2.Sql("USE learn_spark_db");

            //Creating a managed table
            spark2.Sql(@"CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT,
                        distance INT, origin STRING, destination STRING)");

            //useful urls
            //https://stackoverflow.com/questions/50914102/why-do-i-get-a-hive-support-is-required-to-create-hive-table-as-select-error/54552891
            //https://stackoverflow.com/questions/54967186/cannot-create-table-with-spark-sql-hive-support-is-required-to-create-hive-tab

            //how to submit spark job
            //spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --conf spark.sql.catalogImplementation=hive  --master local microsoft-spark-3-0_2.12-1.1.1.jar debug

            //if we get error "The root scratch dir: /tmp/hive on HDFS should be writable. Current permissions are: rw-rw-rw- (on Windows)"
            //https://stackoverflow.com/questions/34196302/the-root-scratch-dir-tmp-hive-on-hdfs-should-be-writable-current-permissions
            //https://stackoverflow.com/questions/40764807/null-entry-in-command-string-exception-in-saveastextfile-on-pyspark/40958969#40958969

            // we can do the same thing using the DataFrame API like this
            var schema = @"date STRING, delay INT, distance INT, origin STRING, destination STRING";
            var flights_df = spark2.Read().Schema(schema).Csv(csvFile);
            flights_df.Write().SaveAsTable("managed_us_delay_flights_tbl");

            spark2.Stop();

            
        }
    }
}
