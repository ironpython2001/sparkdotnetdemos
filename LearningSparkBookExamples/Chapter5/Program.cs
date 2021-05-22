using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using System;
using static Microsoft.Spark.Sql.Functions;


namespace Chapter5
{
    class Program
    {
        static void Main(string[] args)
        {
            Program.example_sparksqludfs();
        }

        static void example_sparksqludfs()
        {
            //how to run
            //spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner  --master local microsoft-spark-3-0_2.12-1.1.1.jar debug

            //page no 114
            var spark = SparkSession.Builder()
                                .AppName("chapter4")
                                .GetOrCreate();
            //string IntToStr(int id)
            //{
            //    return $"The id is {id}";
            //}
            //Func<Column, Column> udfIntToString = Udf<int, string>(id => IntToStr(id));

            //spark.Udf().Register<int?, string, string>(
            //    "my_udf",
            //    (age, name) => name + " with " + ((age.HasValue) ? age.Value.ToString() : "null"));
            //var dataFrame = spark.Sql("SELECT ID from range(1000)");
            //dataFrame.Select(udfIntToString(dataFrame["ID"])).Show();


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
    }
}
