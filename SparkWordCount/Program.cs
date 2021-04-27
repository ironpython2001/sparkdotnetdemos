using System;
using System.Linq;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace SparkWordCount
{
    class Program
    {
        static void Main(string[] args)
        {
            //var text = System.IO.File.ReadAllText("wordcount.txt");

            //var cnt = text.Split("\n").ToList()
            //   .Count(line => line.Trim().ToLower().Contains("to"));
            //Console.WriteLine(cnt);

            var spark = SparkSession.Builder().AppName("wordcount").GetOrCreate();
            var dfText = spark.Read().Text("wordcount.txt");

            Func<Column, Column> udfCount = Udf<string, int>(text =>
            {
                return text.Split("\n").ToList()
                .Count(line => line.Trim().ToLower().Contains("to"));
            });

            //get lines
            var dfLinesContainingTo = dfText.Select(udfCount(dfText["value"])).ToDF("to");
            
            ////print columns
            //foreach(var col in dfLinesContainingTo.Columns())
            //{
            //    Console.WriteLine(col);
            //}
            //dfLinesContainingTo.Show();
            //dfLinesContainingTo.PrintSchema();
            RelationalGroupedDataset rgd = dfLinesContainingTo
                    .Where(dfLinesContainingTo["to"]==1)
                    .GroupBy(dfLinesContainingTo["to"]);
            var df = rgd.Count();
            df.Show();
            spark.Stop();
            //dfLinesContainingTo.Show();
            //int count = 0;
            //foreach (var item in dfLinesContainingTo.Collect())
            //{
            //    count = count + (int)item.Get(0);
            //}
            //Console.WriteLine(count);

        }
    }
}
