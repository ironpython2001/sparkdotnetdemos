using static System.Console;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using Microsoft.Spark.Sql.Types;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Reflection;

namespace DataFrameOperations
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder()
                                  .AppName("ApplySchemaToJsonData")
                                  .GetOrCreate();

            var fire_schema = new StructType(new List<StructField>
            {
               new StructField("CallNumber", new IntegerType(), true),
                new StructField("UnitID", new StringType(), true),
                new StructField("IncidentNumber",new  IntegerType(), true),
                new StructField("CallType", new StringType(), true),
                new StructField("CallDate", new StringType(), true),
                new StructField("WatchDate", new StringType(), true),
                new StructField("CallFinalDisposition", new StringType(), true),
                new StructField("AvailableDtTm", new StringType(), true),
                new StructField("Address", new StringType(), true),
                new StructField("City", new StringType(), true),
                new StructField("Zipcode", new IntegerType(), true),
                new StructField("Battalion", new StringType(), true),
                new StructField("StationArea", new StringType(), true),
                new StructField("Box", new StringType(), true),
                new StructField("OriginalPriority", new StringType(), true),
                new StructField("Priority", new StringType(), true),
                new StructField("FinalPriority", new IntegerType(), true),
                new StructField("ALSUnit",new  BooleanType(), true),
                new StructField("CallTypeGroup", new StringType(), true),
                new StructField("NumAlarms", new IntegerType(), true),
                new StructField("UnitType", new StringType(), true),
                new StructField("UnitSequenceInCallDispatch", new IntegerType(), true),
                new StructField("FirePreventionDistrict", new StringType(), true),
                new StructField("SupervisorDistrict", new StringType(), true),
                new StructField("Neighborhood", new StringType(), true),
                new StructField("Location", new StringType(), true),
                new StructField("RowID", new StringType(), true),
                new StructField("Delay", new FloatType(), true)
            });

            //Use the DataFrameReader interface to read a CSV file
            var firedf = spark.Read()
                //.Option("samplingRatio", 0.001)
                .Option("header", true)
                .Schema(fire_schema)
                .Csv("sf-fire-calls.csv");
            firedf.Show();

            // Page 60 save as a Parquet file
            var parquet_path = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "sf-fire-calls-path");
            firedf.Write().Format("parquet").Save(parquet_path);


            // save as a table
            var parquet_table_path = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "sf-fire-calls-table");
            firedf.Write().Format("parquet").SaveAsTable("sampletable");

            //Projections and filters
            var fewfiredf = firedf.Select("IncidentNumber", "AvailableDtTm", "CallType")
                               .Where(firedf.Col("CallType")!= "Medical Incident");

            fewfiredf.Show(numRows:5);


            //return number of distinct types of calls using countDistinct()
            firedf.Select("CallType").Where(firedf.Col("CallType").IsNotNull())
                .Agg(CountDistinct(firedf.Col("CallType"))).Alias("DistinctCallTypes").Show();

            // filter for only distinct non-null CallTypes from all the rows
            firedf.Select("CallType")
                .Where(firedf.Col("CallType").IsNotNull())
                .Distinct()
                .Show(10);


            //Renaming columns
            var new_fire_df = firedf.WithColumnRenamed("Delay", "ResponseDelayedinMins");
            new_fire_df.Select(new_fire_df.Col("ResponseDelayedinMins"))
                    .Where(new_fire_df.Col("ResponseDelayedinMins") > 5)
                    .Show(5);
             
            //change col datatype and drop col
            var fire_ts_df = new_fire_df
                .WithColumn("IncidentDate",ToTimestamp(new_fire_df.Col("CallDate"), "MM/dd/yyyy"))
                .Drop(new_fire_df.Col("CallDate"))
                .WithColumn("OnWatchDate", ToTimestamp(new_fire_df.Col("WatchDate"), "MM/dd/yyyy"))
                .Drop(new_fire_df.Col("WatchDate"))
                .WithColumn("AvailableDtTS", ToTimestamp(new_fire_df.Col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
                .Drop(new_fire_df.Col("AvailableDtTm"));

            //# Select the converted columns
            fire_ts_df.Select(fire_ts_df.Col("IncidentDate"),fire_ts_df.Col("OnWatchDate"),fire_ts_df.Col( "AvailableDtTS")).Show();

            //get year IncidentDate
            fire_ts_df.Select(Year(fire_ts_df.Col("IncidentDate"))).Distinct()
                .OrderBy(Year(fire_ts_df.Col("IncidentDate"))).Show();

            //Aggregations groupby and order by desc
            fire_ts_df.Select("CallType")
                    .Where(Col("CallType").IsNotNull())
                    .GroupBy("CallType")
                    .Count()
                    .OrderBy(Desc("count"))
                    .Show();

            //page 67 - Other common DataFrame operations
            //descriptive statistical methods like min(), max(), sum()
            fire_ts_df.Select(Sum("NumAlarms"), Avg("ResponseDelayedinMins"), Min("ResponseDelayedinMins"), Max("ResponseDelayedinMins"))
                .Show();

            


            spark.Stop();
        }
    }
}
