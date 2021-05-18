using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using static Microsoft.Spark.Sql.Functions;
using System.Linq;

namespace loganalysis
{
    class Program
    {
        //https://github.com/GoEddie/dotnet-spark-examples/blob/master/examples/chunk-file/chunk-file/Program.cs
        static void Main(string[] args)
        {
            SparkSession spark = SparkSession.Builder().AppName("Apache User Log Processing")
                .GetOrCreate();

            
            
            var file = spark.Read().Text("logs.txt");
            file.Show();


            spark.Udf().Register<string, string[]>("SplitTextFile", (text) => SplitByNewLine(text));

            var s =file.Select(Functions.Explode(Functions.CallUDF("SplitTextFile",file.Col("value"))));
            s.Show();

      //      var exploded = file.Select(Functions.Explode(
      //    Functions.CallUDF("ParseTextFile", file.Col("value"))
      //));

      //      exploded.Show();









        }

        private static string[] SplitByNewLine(string textFile)
        {
            return textFile.Split("\n");
        }

        private static List<string[]> Split(string textFile)
        {
            const int chunkSize = 84;
            int fileLength = textFile.Length;
            var list = new List<string[]>();
            for (var offset = 0; offset < fileLength; offset += chunkSize)
            {
                var row = textFile.Substring(offset, chunkSize);
                list.Add(
                    new string[]
                    {   //yuck! 
                        row.Substring(0, 1), row.Substring(1, 8), row.Substring(9, 15), row.Substring(24, 11),
                        row.Substring(35, 11), row.Substring(46, 1), row.Substring(47, 3), row.Substring(50, 3),
                        row.Substring(54, 1), row.Substring(55, 25), row.Substring(80, 4)
                    }
                );
            }

            return list;
        }
    }
}
