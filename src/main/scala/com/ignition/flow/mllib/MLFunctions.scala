package com.ignition.flow.mllib

import scala.reflect.ClassTag

import org.apache.spark.HashPartitioner
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.{ DataFrame, Row }

import com.ignition.SparkRuntime
import com.ignition.flow.AbstractStep
import com.ignition.types.{ RichRow, RichStructType }

/**
 * Helper functions for MLLib steps.
 *
 * @author Vlad Orzhekhovskiy
 */
trait MLFunctions { self: AbstractStep =>

  /**
   * Converts a data frame into a pair RDD[(key, data)], where key is the row key as defined
   * by the set of grouping fields, and data is a data Vector, as defined by fields.
   */
  protected def toVectors(df: DataFrame, dataFields: Iterable[String],
    groupFields: Iterable[String])(implicit runtime: SparkRuntime): RDD[(Row, Vector)] = {

    val indexMap = df.schema.indexMap
    val fieldIndices = dataFields map indexMap toSeq
    val groupIndices = groupFields map indexMap toSeq

    def valueFunc = (row: Row) => {
      val values = row.subrow(fieldIndices: _*).toSeq map (_.asInstanceOf[Number].doubleValue)
      Vectors.dense(values.toArray)
    }

    partitionByKey(df, groupIndices, valueFunc)
  }

  /**
   * Converts a data frame into a pair RDD[(key, data)], where key is the row key as defined
   * by the set of grouping fields, and data is a LabeledPoint, as defined by label and fields.
   */
  protected def toLabeledPoints(df: DataFrame, labelField: String, dataFields: Iterable[String],
    groupFields: Iterable[String])(implicit runtime: SparkRuntime): RDD[(Row, LabeledPoint)] = {

    val indexMap = df.schema.indexMap

    val labelIndex = indexMap(labelField)
    val fieldIndices = dataFields map indexMap toSeq
    val groupIndices = groupFields map indexMap toSeq

    def valueFunc = (row: Row) => {
      val label = row(labelIndex).asInstanceOf[Number].doubleValue
      val features = row.subrow(fieldIndices: _*).toSeq map (_.asInstanceOf[Number].doubleValue)
      val vector = Vectors.dense(features.toArray)
      LabeledPoint(label, vector)
    }

    partitionByKey(df, groupIndices, valueFunc)
  }

  /**
   * Converts the data frame into an RDD[(key, value)] where key is defined by the group
   * field indices, and value is computed for each row by the supplied function.
   */
  private def partitionByKey[T: ClassTag](df: DataFrame, groupIndices: Seq[Int],
    func: Row => T)(implicit runtime: SparkRuntime): RDD[(Row, T)] = df mapPartitions { rows =>
    rows map { row =>
      val key = row.subrow(groupIndices: _*)
      val value = func(row)
      (key, value)
    }
  } partitionBy (new HashPartitioner(ctx.sparkContext.defaultParallelism))
}