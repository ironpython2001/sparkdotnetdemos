using static System.Console;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using Microsoft.Spark.Sql.Types;
using System.Collections.Generic;
using System.Linq;

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

            //columns
            foreach (var col in blogs_df.Columns())
            {
                WriteLine(col);
            }

            // Access a particular column with col and it returns a Column type
            WriteLine(blogs_df.Col("Id"));

            // Use an expression to compute a value
            blogs_df.Select(Expr("Hits * 2")).Show(2);
            // or use col to compute value
            blogs_df.Select(blogs_df.Col("Hits") * 2).Show(2);


            // Use an expression to compute big hitters for blogs
            // This adds a new column, Big Hitters, based on the conditional expression
            blogs_df.WithColumn("Big Hitters", blogs_df.Col("Hits") > 10000).Show();

            // Concatenate three columns, create a new column, and show the
            // newly created concatenated column
            blogs_df.WithColumn("AuthorsId", Concat(blogs_df.Col("First"), blogs_df.Col("Last"), blogs_df.Col("Id")))
                .Select("AuthorsId")
                .Show(4);


            // These statements return the same value, showing that
            // expr is the same as a col method call
            blogs_df.Select(Expr("Hits")).Show(2);
            blogs_df.Select(blogs_df.Col("Hits")).Show(2);
            blogs_df.Select("Hits").Show(2);

            // Sort by column "Id" in descending order
            blogs_df.Sort(blogs_df.Col("Id").Desc()).Show();


            //create row

            var blog_row = new GenericRow(new object[] { 6, "Reynold", "Xin", "https://tinyurl.6", 255568 });
            WriteLine(blog_row[1].ToString());


            //create DataFrames from row objects
            var blog_rows = new List<GenericRow>
            {
                 new GenericRow(new object[]{ "Matei Zaharia", "CA" })
            };
            var authors_df = spark.CreateDataFrame(blog_rows, new StructType(new List<StructField> 
                { 
                    new StructField("Author",new StringType()), 
                    new StructField("State",new StringType())
                  }));
            authors_df.Show();

            spark.Stop();
        }
    }
}
