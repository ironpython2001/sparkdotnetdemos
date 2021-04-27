using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace wordoccurcount
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().AppName("wordoccurcount").GetOrCreate();
            var dfFile = spark.Read().Text("wordcount.txt");

            var udf = Udf<string, List<string>>(text =>
            {
                List<string> res = new List<string>();
                var lines = text.Split("\n").ToList();
                foreach(var line in lines)
                {
                    var words = line.Split(" ").ToList();
                    foreach (var word in words)
                    {
                        res.Add(word);
                    }
                }
                return res;
            });
            var dfLines = dfFile.Select(Functions.Split(dfFile["value"],"\n")).ToDF("lines");
            dfLines.PrintSchema();
            dfLines.Show();
            //var udf1 = Udf<string[], string[]>(line=>
            //{
            //    //foreach (var line in lines.ToList())
            //    //{
            //        return line.Split(" ");
            //    //}
            //});
            //Functions.(Split()
            //var dfWords = dfLines.Select(udf1(dfLines["lines"]));
            //dfWords.Show();


            spark.Stop();
        }
    }
}
