package workshop.wordcount

import java.time.Clock

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

object WordCount {
  val log: Logger = LogManager.getRootLogger
  implicit val clock: Clock = Clock.systemDefaultZone()

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = initSpark

    runTask(spark)

    spark.stop()
  }


  def initSpark = {
    val conf = ConfigFactory.load
    log.setLevel(Level.INFO)
    val spark = SparkSession.builder.appName("Spark Word Count").master("local[*]").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)
    spark
  }


  def runTask(spark: SparkSession): Array[Row] = {

    log.info("Reading data: ")
    log.info("Writing data: ")

    val df = spark.read.textFile("../spark-basic/data/A perfect day.txt")
      .withColumn("word", explode(split(col("value"), " ")))
      .select(regexp_replace(upper(col("word")),"\\,|\\.|\\?", "").alias("word"))
      .groupBy("word")
      .count()
      .orderBy("word")

    df.collect()
  }


  def runTask2(spark: SparkSession): Unit = {
    log.info("Reading data: ")
    log.info("Writing data: ")

    val df = spark.sparkContext.textFile("../spark-basic/data/A perfect day.txt")
    // Split it up into words
    val words = df.flatMap(line => line.replaceAll(",", "")
      .replaceAll("\\;", "")
      .replaceAll("\\.", "")
      .replaceAll("\\?", "").split(" "))

    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }

    val tuples = counts.collect().toMap
    print(tuples)

    log.info("Application Done: " + spark.sparkContext.appName)
  }

}
