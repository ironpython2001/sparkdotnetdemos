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

            spark.Stop();
        }
    }
}
