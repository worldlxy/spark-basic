package workshop.wordcount

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}
import workshop.wordcount.WordCount.{initSpark, runTask}

class WordCountTest extends FunSuite with BeforeAndAfter{

  var spark: SparkSession = _

  before {
    spark = initSpark
  }

  after {
    spark.stop()
  }

  test("current file contains 70 distinct words") {
    val result = runTask(spark)

    assertResult(70)(result.length)
  }

  test("word count result is right!") {
    val result = runTask(spark)

    assertResult(Row("A", 12))(result(0))
    assertResult(Row("COLORS", 1))(result(10))
    assertResult(Row("YOUR", 1))(result(69))
  }

}
