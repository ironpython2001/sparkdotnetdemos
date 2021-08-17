using System;
using System.Linq;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Sql;
using func = Microsoft.Spark.Sql.Functions;
using DataFrame = Microsoft.Spark.Sql.DataFrame;
using Arrow = Apache.Arrow;

namespace Chapter5
{
    //https://stackoverflow.com/questions/66795033/how-to-pass-array-column-as-argument-in-vectorudf-in-net-spark
    //http://persianprogrammer.com/Education/Index/-NET-for-Apache%C2%AE-Spark%E2%84%A2-In-Memory-DataFrame-Support
    public class StackOverFlowExample
    {
        public void run()
        {
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
            Func<Column,Column> dataFrameVector = DataFrameFunctions.VectorUdf<Int64DataFrameColumn, Int64DataFrameColumn>((id) => id + id.Length);

            //dataFrame = dataFrame.WithColumn("arrowVectorUdfId", arrowVectorUdf(dataFrame["id"]));
            //dataFrame.Show();
            //if (dataFrame.Col("id") is Int64DataFrameColumn)
            //{
            //    Console.WriteLine();
            //}

            dataFrame.PrintSchema();
            dataFrame = dataFrame.WithColumn("arrowVectorUdf", arrowVectorUdf(dataFrame["array"]));

            dataFrame = dataFrame.WithColumn("dataFrameVector", dataFrameVector(dataFrame["array"]));

            var d = dataFrame.Select(dataFrame["dataFrameVector"]);
            d.Show();
        }
    }
}
