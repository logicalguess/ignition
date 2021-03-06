package com.ignition.frame

import scala.reflect.ClassTag
import org.apache.spark.{ Accumulator, AccumulatorParam, SparkContext }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import com.ignition.FlowRuntime
import org.slf4j.LoggerFactory

/**
 * Encapsulates the spark context and SQL context and provides helper
 * functions to manage Spark runtime environment.
 *
 * @author Vlad Orzhekhovskiy
 */
trait SparkRuntime extends FlowRuntime with Serializable {
  def sc: SparkContext
  def ctx: SQLContext
  
  /**
   * Set once, read-only variables.
   */
  val vars = new Serializable {
    private var bcMap: Map[String, Broadcast[_]] = Map.empty

    def names: Set[String] = bcMap.keySet

    def apply(name: String): Any = bcMap(name).value

    def getAs[T: ClassTag](name: String): T = bcMap(name).asInstanceOf[Broadcast[T]].value

    def update[T: ClassTag](name: String, value: T): Unit = {
      bcMap.get(name) foreach (_.destroy)
      val bc = sc.broadcast(value)
      bcMap += name -> bc
    }

    def drop(name: String): Unit = {
      bcMap.get(name) foreach (_.destroy)
      bcMap -= name
    }

    def -= = drop _
  }

  /**
   * Accumulators.
   */
  val accs = new Serializable {
    private var acMap: Map[String, Accumulator[_]] = Map.empty

    def names: Set[String] = acMap.keySet

    def apply(name: String) = acMap(name).value

    def getAs[T: ClassTag](name: String): T = acMap(name).asInstanceOf[Accumulator[T]].value

    def getLocal(name: String) = acMap(name).localValue

    def getLocalAs[T: ClassTag](name: String): T = acMap(name).asInstanceOf[Accumulator[T]].localValue

    def add[T: ClassTag](name: String, value: T) = acMap(name).asInstanceOf[Accumulator[T]] += value

    def update[T: ClassTag](name: String, value: T)(implicit param: AccumulatorParam[T]) = {
      acMap get (name) map (_.asInstanceOf[Accumulator[T]].setValue(value)) getOrElse {
        val acc = sc.accumulator(value, name)
        acMap += name -> acc
      }
    }

    def drop(name: String): Unit = acMap -= name
  }
}

/**
 * The default implementation of SparkRuntime.
 *
 * @author Vlad Orzhekhovskiy
 */
class DefaultSparkRuntime(@transient val ctx: SQLContext, val previewMode: Boolean = false)
    extends SparkRuntime {
  
  @transient protected val log = LoggerFactory.getLogger(getClass)

  @transient val sc = ctx.sparkContext
}