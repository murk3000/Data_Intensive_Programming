package dip22.ex2

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, DataType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{array, asc, col, desc, explode, expr, max, min, round, sum, udf, year}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level


object Ex2Main extends App {
  // Create the Spark session
	val spark = SparkSession.builder()
                          .appName("ex2")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate()

  // suppress informational log messages related to the inner working of Spark
  spark.sparkContext.setLogLevel("ERROR")

  spark.conf.set("spark.sql.shuffle.partitions", "5")



  printTaskLine(1)
  // Task 1: File "data/rdu-weather-history.csv" contains weather data in csv format.
  //         Study the file and read the data into DataFrame weatherDataFrame.
  //         Let Spark infer the schema. Study the schema.
  val weatherDataFrame: DataFrame = spark.read.options(Map("inferSchema"->"true", "header"->"true")).csv("data/rdu-weather-history.csv")

  // Study the schema of the DataFrame:
  weatherDataFrame.printSchema()



  printTaskLine(2)
  // Task 2: print three first elements of the data frame to stdout
  val weatherSample: Array[Row] = weatherDataFrame.head(3)
  weatherSample.foreach(println)



  printTaskLine(3)
  // Task 3: Find min and max temperatures from the whole DataFrame
  val minTemp: Double = weatherDataFrame.agg(min("temperaturemin") as "minTemp").head.getDouble(0)
  val maxTemp: Double = weatherDataFrame.agg(max("temperaturemax") as "maxTemp").head.getDouble(0)
  // In case you want to do it with sql, a bit more wordy
//  weatherDataFrame.createOrReplaceTempView("weather")
//  val minTemp: Double = spark.sql("SELECT min(temperaturemin) FROM weather").head().getDouble(0)
//  val maxTemp: Double = spark.sql("SELECT max(temperaturemax) FROM weather").head().getDouble(0)

  println(s"Min temperature is ${minTemp}")
  println(s"Max temperature is ${maxTemp}")



  printTaskLine(4)
  // Task 4: Add a new column "year" to the weatherDataFrame.
  // The type of the column is integer and value is calculated from column "date".
  // You can use function year from org.apache.spark.sql.functions
  // See documentation: "def year" from https://spark.apache.org/docs/3.3.0/api/scala/org/apache/spark/sql/functions$.html
  // import org.apache.spark.sql.functions.year

  val weatherDataFrameWithYear: DataFrame = weatherDataFrame.withColumn("year", year(col("date")))
  weatherDataFrameWithYear.printSchema()


  printTaskLine(5)
  // Task 5: Find min and max temperature for each year
//  val aggregatedDF: DataFrame = weatherDataFrameWithYear.groupBy("year").agg(Map("temperaturemin"->"min", "temperaturemax"->"max"))
  val aggregatedDF: DataFrame = weatherDataFrameWithYear.groupBy("year").agg(
  expr("min(temperaturemin) as min_temp"),
  expr("max(temperaturemax) as max_temp"))

  aggregatedDF.printSchema()
  aggregatedDF.collect().foreach(println)



  printTaskLine(6)
  // Task 6: Expansion of task 5.
  //         In addition to the min and max temperature for each year find out also the following:
  //         - count for how many records there are for each year
  //         - the average wind speed for each year (rounded to 2 decimal precision)
  val task6DF: DataFrame = weatherDataFrameWithYear.groupBy("year").agg(
    expr("min(temperaturemin) as min_temp"),
    expr("max(temperaturemax) as max_temp"),
    expr("avg(avgwindspeed) as avg_windspeed")).withColumn("avg_windspeed", round(col("avg_windspeed"), 2))

  task6DF.show()



  // Stop the Spark session
  spark.stop()

  // Helper function to separate the task outputs from each other
  def printTaskLine(taskNumber: Int): Unit = {
    println(s"\n======\nTask $taskNumber\n======")
  }
}
