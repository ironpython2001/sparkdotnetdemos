using System;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace HelloSpark
{
    class Program
    {
        private static int AddAmount = 100;
        private static Int64DataFrameColumn Add100(Int64DataFrameColumn id)
        {
            return id.Add(AddAmount);
        }
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();
            Func<Column, Column> udfIntToString = Udf<int, string>(id => { return $"The id is {id}"; ; });
            var dataFrame = spark.Sql("SELECT ID from range(1000)");
            dataFrame.Select(udfIntToString(dataFrame["ID"])).Show(); 
        }
    }
}
