using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using System;
using static Microsoft.Spark.Sql.Functions;
using static Microsoft.Spark.Sql.DataFrameFunctions;
using Microsoft.Data.Analysis;
using System.Linq;


namespace Chapter5
{
    class Program
    {
        static void Main(string[] args)
        {
            //Program.example_sparksqludfs();
            //Program.example_vectorudfs();
            //sample();

            //new SampleVectorUdafs().run();
            new StackOverFlowExample().run();
        }

        static void example_sparksqludfs()
        {
            //dotnet spark examples
            //https://the.agilesql.club/
            //how to run
            //spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner  --master local microsoft-spark-3-0_2.12-1.1.1.jar debug

            //page no 114
            var spark = SparkSession.Builder()
                                .AppName("chapter5")
                                .GetOrCreate();

            string IntToStr(int id)
            {
                return $"The id is {id}";
            }
            Func<Column, Column> udfIntToString = Udf<int, string>(id => IntToStr(id));

            var df1 = spark.Range(1, 100);
            df1.Select(udfIntToString(df1.Col("ID"))).Show();


            //Func<Column, Column> cubed = Udf<int, int>(id => { return id * id; });
            //reference url
            //https://github.com/dotnet/spark/blob/main/examples/Microsoft.Spark.CSharp.Examples/Sql/Batch/Basic.cs
            spark.Udf().Register<int, int>("cubed", (id) => { return (id * id * id); });
            spark.Sql("select id,cubed(id) from range(12)");
            spark.Sql("SELECT ID from range(1000)").CreateOrReplaceTempView("udf_test");
            spark.Sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").Show();

            //page no 115
            //Pandas UDFs or vectorized UDFs
            //.net VectorUdf

            spark.Stop();
        }

        static void example_vectorudfs()
        {
            //how to run
            //spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner  --master local microsoft-spark-3-0_2.12-1.1.1.jar debug
            //https://devblogs.microsoft.com/dotnet/net-for-apache-spark-in-memory-dataframe-support/


            //page no 116
            //Vectorized UDFs or Pandas UDFs
            //.net VectorUdf
            var spark = SparkSession.Builder()
                .Config("spark.sql.warehouse.dir", "file:///e:/tmp")
                    .AppName("chapter5")
                    .GetOrCreate();


            Func<Column, Column> cubed_udf = VectorUdf<Int64DataFrameColumn, Int64DataFrameColumn>((a) => { return a ; });
            var df = spark.Range(1, 10);
            var df1 = df.Select(cubed_udf(df["id"])).As("df1");
            //not working for me
            df1.Show();
            foreach( var i in df1.Collect())
            {
                Console.WriteLine(i);
            }
            df.Show();
            spark.Stop();
        }



        static void sample()
        {
            SparkSession spark = SparkSession
                .Builder()
                .AppName("sample")
                .GetOrCreate();

            var dataFrame = spark.Range(0, 100).Repartition(4);

            //var array20 = Udf<int, int[]>(
            //    (col1) => Enumerable.Range(0, col1).ToArray());

            //dataFrame = dataFrame.WithColumn("array", array20(dataFrame["id"]));

            
            // Microsoft.Data.Analysis
            var array21 = DataFrameFunctions.VectorUdf<Int64DataFrameColumn, Int64DataFrameColumn>((id) => id + id.Length);
            dataFrame = dataFrame.WithColumn("array", array21(dataFrame["id"]));
            dataFrame.Select(dataFrame.Col("array")).Show();

        }

    }
}
