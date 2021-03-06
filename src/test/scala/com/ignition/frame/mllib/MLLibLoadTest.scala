package com.ignition.frame.mllib

import scala.util.Random

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ Row, SQLContext }

import com.ignition.{ ExecutionException, TestDataHelper }
import com.ignition.frame.{ DataGrid, DefaultSparkRuntime }
import com.ignition.types.{ RichStructType, double, fieldToRichStruct, int, long, string }

object MLLibLoadTest extends App with TestDataHelper {

  val sc = new SparkContext("local[4]", "test", new SparkConf)
  implicit val ctx = new SQLContext(sc)
  import ctx.implicits._
  implicit val runtime = new DefaultSparkRuntime(ctx)

  testColumnStats(100, 100000)
  testCorrelation(100, 100000)
  testRegression(100, 10000)

  /**
   * MLLib ColumnStats
   */
  def testColumnStats(keyCount: Int, rowCount: Int) = {
    val schema = string("key1") ~ int("key2") ~ int("intVal") ~ double("dblVal") ~ long("lngVal")

    println("ColumnStats - preparing data...")
    val keys = (1 to keyCount) map (_ => (Random.nextLong.toString, Random.nextInt))
    assert(keys.size == keyCount)

    val rows = (1 to rowCount) map { _ =>
      val key = keys(Random.nextInt(keys.size))
      Row(key._1, key._2, randomInt, randomDouble, randomLong)
    }
    assert(rows.size == rowCount)

    val grid = DataGrid(schema, rows)
    val stats = ColumnStats() groupBy ("key1", "key2") add ("intVal", "dblVal", "lngVal")
    grid --> stats
    println("ColumnStats - computing...")

    benchmark(f"ColumnStats for $rowCount%,d rows and $keyCount%,d keys") {
      stats.output
    }
  }

  /**
   * MLLib Correlation.
   */
  def testCorrelation(keyCount: Int, rowCount: Int) = {
    val schema = string("key1") ~ int("key2") ~ int("intVal") ~ double("dblVal") ~ long("lngVal")

    println("Correlation - preparing data...")
    val keys = (1 to keyCount) map (_ => (Random.nextLong.toString, Random.nextInt))
    assert(keys.size == keyCount)

    val rows = (1 to rowCount) map { _ =>
      val key = keys(Random.nextInt(keys.size))
      Row(key._1, key._2, randomInt, randomDouble, randomLong)
    }
    assert(rows.size == rowCount)

    val grid = DataGrid(schema, rows)
    val stats = Correlation() % ("intVal", "dblVal", "lngVal") groupBy ("key1", "key2")
    grid --> stats
    println("Correlation - computing...")

    benchmark(f"Correlation for $rowCount%,d rows and $keyCount%,d keys") {
      stats.output
    }
  }

  def testRegression(keyCount: Int, rowCount: Int) = {
    val schema = string("key1") ~ int("key2") ~
      double("label") ~ double("feature1") ~ double("feature2")

    println("Regression - preparing data...")
    val keys = (1 to keyCount) map (_ => (Random.nextLong.toString, Random.nextInt))
    assert(keys.size == keyCount)

    val rows = (1 to rowCount) map { _ =>
      val key = keys(Random.nextInt(keys.size))
      val (f1, f2) = (randomDouble, randomDouble)
      val label = 3 * f1 + 2 * f2

      Row(key._1, key._2, label, f1, f2)
    }
    assert(rows.size == rowCount)

    val grid = DataGrid(schema, rows)
    val reg = Regression("label") features ("feature1", "feature2") groupBy ("key1", "key2")
    grid --> reg
    println("Regression - computing...")

    benchmark(f"Regression for $rowCount%,d rows and $keyCount%,d keys") {
      reg.output
    }
  }

  def benchmark[T](name: String)(block: => T): T = {
    val start = System.currentTimeMillis
    val result = block
    val end = System.currentTimeMillis

    println(f"[$name] evaluated in ${end - start}%,d ms")
    result
  }

  private def randomInt = Random.nextInt(1000000) - 500000
  private def randomDouble = randomInt / 10000.0
  private def randomLong = randomInt * 100 toLong
}