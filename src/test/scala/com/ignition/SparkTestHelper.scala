package com.ignition

import scala.concurrent.blocking

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{ Milliseconds, StreamingContext }

import com.ignition.util.ConfigUtils
import com.ignition.util.ConfigUtils.RichConfig

/**
 * Spark test helper.
 *
 * @author Vlad Orzhekhovskiy
 */
trait SparkTestHelper extends BeforeAllAfterAll {

  private val config = ConfigUtils.getConfig("spark.test")

  protected def sparkConf = new SparkConf(true).
    set("spark.sql.retainGroupColumns", "false").
    set("spark.app.id", "ignition-test")
  //    set("spark.cassandra.connection.host", CassandraBaseTestHelper.host).
  //    set("spark.cassandra.connection.native.port", CassandraBaseTestHelper.port.toString).
  //    set("spark.cassandra.connection.rpc.port", CassandraBaseTestHelper.thriftPort.toString)

  val masterUrl = config.getString("master-url")
  val appName = config.getString("app-name")
  val batchDuration = config.getTimeInterval("streaming.batch-duration")

  protected def createSparkContext: SparkContext =
    new SparkContext(masterUrl, appName, sparkConf)

  protected def createSQLContxt(sc: SparkContext): SQLContext = new SQLContext(sc)

  protected def createStreamingContext(sc: SparkContext): StreamingContext = {
    val cfg = config.getConfig("streaming")
    val ssc = new StreamingContext(sc, Milliseconds(batchDuration.getMillis))
    val checkpointDir = cfg.getString("checkpoint-dir")
    ssc.checkpoint(checkpointDir)
    ssc
  }

  protected def clearContext(sc: SparkContext) = blocking {
    sc.stop
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.master.port")
  }
}