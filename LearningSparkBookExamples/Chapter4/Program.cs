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
            //Samples1();
            //CreatingManagedTable();
            CreatingUnManagedTable();
        }

        static void Samples1()
        {
            //How to Run
            //spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner  --master local microsoft-spark-3-0_2.12-1.1.1.jar debug

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
        }

        static void CreatingManagedTable()
        {
            //How to Run
            //spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner  --master local microsoft-spark-3-0_2.12-1.1.1.jar debug
            var csvFile = "departuredelays.csv";

            //SQL Tables and Views
            //page no 90
            var spark = SparkSession.Builder()
                                .EnableHiveSupport()
                                .GetOrCreate();

            /*
             * DEVELOPER COMMENTS:
             * Some times while executing the "spark2.Sql("CREATE DATABASE learn_spark_db")"
             * WE WILL GET THE BELOW ERROR 
             * /TMP/hive on HDFS should be writable. The current permissions are: RW RW RW - (on Windows)
             * 
             * To solve the above error . On the command prompt (if you are running windows) run the below command
             * %HADOOP_HOME%\bin\winutils.exe ls e:\hive(where e:\hive is my hive directory)
             * the output of the above command will be like below 
             * --------- 1 domainname\username domainname\Domain Users 0 May 20 2021 e:\hive
             * after running the above command I also observed that a new directory got created 
             * in my E drive "E:\tmp\hive\loggedinusername"
             * 
             * IF YOU ARE CONNECTED TO OFFICE NETWORK . RUN THE ABOVE COMMAND 
             * AFTER CONNECTING TO VPN FOR YOUR OFFICE NETWORK . SO THAT WE WILL BE ON THE SAME DOMAIN
             * This is an issue with winutils
             * 
             * I added some useful URL's that helped me to solve the above issue
            */

            //Creating SQL Databases and Tables
            //creating managed table
            spark.Sql("CREATE DATABASE learn_spark_db");
            spark.Sql("USE learn_spark_db");

            //Creating a managed table
            spark.Sql(@"CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT,
                        distance INT, origin STRING, destination STRING)");



            // we can do the same thing using the DataFrame API like this
            //var schema = @"date STRING, delay INT, distance INT, origin STRING, destination STRING";
            //var flights_df = spark.Read().Schema(schema).Csv(csvFile);
            //flights_df.Write().SaveAsTable("managed_us_delay_flights_tbl");

            spark.Stop();
        }

        static void CreatingUnManagedTable()
        {
            //page no 91
            var spark = SparkSession.Builder()
                                .EnableHiveSupport()
                                .GetOrCreate();

            var csvFile = "departuredelays.csv";
            spark.Sql(@$"CREATE TABLE us_delay_flights_tbl(date STRING, delay INT,
                      distance INT, origin STRING, destination STRING)
                     USING csv OPTIONS(PATH '{csvFile}')");

             //creating views
             


            spark.Stop();
        }

    }
}
