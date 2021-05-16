using static System.Console;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using Microsoft.Spark.Sql.Types;
using System.Collections.Generic;

namespace ApplySchemaToJsonData
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder()
                                  .AppName("ApplySchemaToJsonData")
                                  .GetOrCreate();

            var schema = new StructType(new List<StructField>
            {
                new StructField("Id", new IntegerType(),false),
                new StructField("First", new StringType(),false),
                new StructField("Last", new StringType(),false),
                new StructField("Url", new StringType(),false),
                new StructField("Published", new StringType(),false),
                new StructField("Hits", new IntegerType(),false),
                new StructField("Campaigns", new ArrayType(new StringType()),false),
            });

            // Create a DataFrame by reading from the JSON file 
            // with a predefined schema
            //please not the format of the json
            var blogs_df = spark.Read()
                .Schema(schema)
                .Json(@"data.json");

            blogs_df.Show();
            blogs_df.PrintSchema();

            spark.Stop();
        }
    }
}
