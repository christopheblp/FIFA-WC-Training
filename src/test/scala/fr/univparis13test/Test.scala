package fr.univparis13test

import java.sql.Date

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

case class Results(date: Date,
                   home_team: String,
                   away_team: String,
                   home_score: Int,
                   away_score: Int,
                   tournament: String,
                   city: String,
                   country: String,
                   neutral: Boolean)

class Test extends FunSuite with BeforeAndAfterAll {

  val spark: SparkSession = SparkSession.builder
    .appName("SparkAppExample")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  override def beforeAll(): Unit = {

  }


  test("Load JSON file") {
    loadDfFromCSV
  }

  def loadDfFromCSV: DataFrame = {
    spark.read
      .option("delimeter", ",")
      .option("header", "true")
      .csv("src/test/resources/results.csv")
  }

  def getDSFromDF(dataframe: DataFrame): Dataset[Results] = {
    dataframe.select(
      $"date",
      $"home_team",
      $"away_team",
      $"home_score".cast(IntegerType),
      $"away_score".cast(IntegerType),
      $"tournament",
      $"city",
      $"country",
      $"neutral")
      .as[Results]
  }

  test("Convert to dataset") {

    val frame: DataFrame = loadDfFromCSV
    val ds: Dataset[Results] = getDSFromDF(frame)
    //    ds.show(10, truncate = false)

  }

  test("Add a column to indicate if the home team has won the match") {

    val frame: DataFrame = loadDfFromCSV
    val frame2 = frame.withColumn("Result", when($"home_score" > $"away_score", "Win")
      .when($"home_score" === $"away_score", "draw").otherwise("lose"))
    frame2.show(10, false)

  }

  test("Best national team ever") {

    val frame: DataFrame = loadDfFromCSV
    val frame2 = frame.withColumn("Result", when($"home_score" > $"away_score", "Win")
      .when($"home_score" === $"away_score", "draw").otherwise("lose"))

    frame2.createOrReplaceTempView("results")

    spark.sql("SELECT home_team, count(Result) as Wins FROM results WHERE Result='Win' GROUP BY home_team ORDER BY Wins DESC").show(10)

  }

  test("Add columns with concatenated scores") {

    val frame: DataFrame = loadDfFromCSV
    val frame2 = frame.withColumn("concatResult", concat($"home_score", lit("-"), $"away_score"))

    frame2.show(10, false)

  }

  test("Best team per year") {
    val frame: DataFrame = loadDfFromCSV
    frame.groupBy(year($"date"))
  }


  override def afterAll(): Unit = {
    spark.stop
    assert(spark.sparkContext.isStopped)
  }

}
