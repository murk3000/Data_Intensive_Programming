
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{round, sum, col, count, lpad, max, rtrim, substring, substring_index, to_date, when}
import org.apache.spark.sql.types.DateType

val spark = SparkSession.builder()
  .appName("ex3")
  .config("spark.driver.host", "localhost")
  .master("local")
  .getOrCreate()

// suppress log messages related to the inner working of Spark
spark.sparkContext.setLogLevel("ERROR")

spark.conf.set("spark.sql.shuffle.partitions", "5")

val  retailerDataFrame: DataFrame = spark.read.options(
  Map("inferSchema"->"true", "header"->"true", "delimiter"->";")
).csv("Documents/DIP/Exercises/ex3/data/sales_data_sample.csv")

retailerDataFrame.select("ORDERDATE").show(50)

//retailerDataFrame.withColumns(
//  Map("orderdt"->col("ORDERDATE"))
//).select("orderdt").show()

//val best10DaysDF: DataFrame = retailerDataFrame.withColumn(
//  "orderdt", substring_index(col("ORDERDATE")," ",1)
//).withColumn(
//  "sales", col("QUANTITYORDERED")*col("PRICEEACH")
//).groupBy("orderdt").agg(
//  count("ORDERNUMBER") as "count",
//  round(sum("sales"), 2) as "sales"
//).sort(col("sales").desc).limit(10)
//
//best10DaysDF.show(10)

//
//case class Sales(year: Int, euros: Int) {
//  override def toString : String = {
//    return "("+year+", "+euros+")"
//  }
//}
//
//import spark.implicits._
//val salesList = List(Sales(2015, 325), Sales(2016, 100), Sales(2017, 15), Sales(2018, 1000),
//  Sales(2019, 50), Sales(2020, 750), Sales(2021, 950), Sales(2022, 400))
//val salesDS: Dataset[Sales] = spark.createDataset[Sales](salesList)
//
////print(salesDS.schema)
//salesDS.toDF("year", "euros").agg(max("euros"))

spark.close()