using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using System;
using static Microsoft.Spark.Sql.Functions;
using static Microsoft.Spark.Sql.DataFrameFunctions;
using Microsoft.Data.Analysis;

namespace Chapter5
{
    class Program
    {
        static void Main(string[] args)
        {
            //Program.example_sparksqludfs();
            Program.example_vectorudfs();
        }

        static void example_sparksqludfs()
        {
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
                 .Config("spark.sql.shuffle.partitions", "3")
                    .AppName("chapter5")
                    .GetOrCreate();


            Func<Column, Column> cubed_udf = VectorUdf<Int64DataFrameColumn, Int64DataFrameColumn>((a) => { return a * a * a; });
            var df = spark.Range(1, 4);
            df.Select(cubed_udf(df["id"])).Show();

            spark.Stop();
        }


    }
}
