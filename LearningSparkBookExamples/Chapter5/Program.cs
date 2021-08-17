using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using System;
using static Microsoft.Spark.Sql.Functions;
using static Microsoft.Spark.Sql.DataFrameFunctions;
using Microsoft.Data.Analysis;
using System.Linq;

using func = Microsoft.Spark.Sql.Functions;
using DataFrame = Microsoft.Spark.Sql.DataFrame;
using Arrow = Apache.Arrow;

namespace Chapter5
{
    class Program
    {
        static void Main(string[] args)
        {
            //Program.example_sparksqludfs();
            //Program.example_vectorudfs();
            Program.semiworkingexamples();
            ///sample();

            //new SampleVectorUdafs().run();
            //new StackOverFlowExample().run();
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
                    .Config("spark.driver.memory", "4g")
                    .AppName("chapter5")
                    .GetOrCreate();


            Func<Column, Column> cubed_udf = VectorUdf<Int64DataFrameColumn, Int64DataFrameColumn>((a) => { return a; });
            var df = spark.Range(1, 10);
            //not working for me
            df.Select(cubed_udf(df["id"])).Show();
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


        public static void semiworkingexamples()
        {
            //https://translate.google.com/translate?hl=en&sl=zh-CN&u=https://www.coder.work/article/7554678&prev=search&pto=aue
            //http://persianprogrammer.com/Education/Index/-NET-for-Apache%C2%AE-Spark%E2%84%A2-In-Memory-DataFrame-Support
            SparkSession spark = SparkSession
                .Builder()
                .AppName("sample")
                .GetOrCreate();

            DataFrame dataFrame = spark.Range(0, 100).Repartition(4);

            Func<Column, Column> array20 = func.Udf<int, int[]>(
                (col1) => Enumerable.Range(0, col1).ToArray());

            dataFrame = dataFrame.WithColumn("array", array20(dataFrame["id"]));

            // Apache Arrow
            var arrowVectorUdf = ArrowFunctions.VectorUdf<Arrow.UInt64Array, Arrow.Int64Array>((id) =>
            {
                var int32Array = new Arrow.Int64Array.Builder();
                var count = id.Length;
                foreach (var item in id.Data.Children)
                {
                    int32Array.Append(item.Length + count);
                }
                return int32Array.Build();
            });

            // Microsoft.Data.Analysis
            var dataFrameVector = DataFrameFunctions.VectorUdf<Int64DataFrameColumn, Int64DataFrameColumn>((id) => id + id.Length);
            dataFrame = dataFrame.WithColumn("arrowVectorUdfId", arrowVectorUdf(dataFrame["id"]));
            var df1 = dataFrame.Select();
            df1.Show();
            dataFrame = dataFrame.WithColumn("dataFrameVectorId", dataFrameVector(dataFrame["id"]));
            var df2 = dataFrame.Select();
            df2.Show();


            dataFrame = dataFrame.WithColumn("arrowVectorUdf", arrowVectorUdf(dataFrame["array"]));
            var df3 = dataFrame.Select();
            df3.Show();
            dataFrame = dataFrame.WithColumn("dataFrameVector", dataFrameVector(dataFrame["array"]));


            var df5 = spark.Range(1, 10);
           
            var cubed_udf = DataFrameFunctions.VectorUdf<Int32DataFrameColumn,Int32DataFrameColumn>((a) => a = 3*a);
            var df6 = df5.WithColumn("arrowVectorUdfId", cubed_udf(df5["id"]));
            df6.PrintSchema();
            df6.Select("arrowVectorUdfId").Show();
            var df7 = df6.Select();
            df7.Show();
            //spark.Stop();
        }


    }
}
