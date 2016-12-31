package com.ignition.frame

import scala.xml.{ Elem, Node }

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{ Column, DataFrame }
import org.apache.spark.sql.catalyst.SqlParser
import org.json4s.JValue
import org.json4s.JsonDSL.{ jobject2assoc, option2jvalue, pair2Assoc, pair2jvalue, string2jvalue }
import org.json4s.jvalue2monadic

import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

import JoinType.{ INNER, JoinType }

/**
 * DataFrame join type.
 */
object JoinType extends Enumeration {
  type JoinType = Value

  val INNER = Value("inner")
  val OUTER = Value("outer")
  val LEFT = Value("left_outer")
  val RIGHT = Value("right_outer")
  val SEMI = Value("semijoin")
}

/**
 * Performs join of the two data frames.
 * In row conditions, if there is ambiguity in a field's name, use "input0" and "input1"
 * prefixes for the first and second input respectively.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Join(condition: Option[String], joinType: JoinType) extends FrameMerger/*(2)*/ {
  import Join._

  override def inputCount: Int = 2

  def joinType(jt: JoinType) = copy(joinType = jt)

  protected def compute(args: IndexedSeq[DataFrame])(implicit runtime: SparkRuntime): DataFrame = {

    val df1 = optLimit(args(0), runtime.previewMode).as('input0)
    val df2 = optLimit(args(1), runtime.previewMode).as('input1)

    condition map { c =>
      df1.join(df2, new Column(SqlParser.parseExpression(c)), joinType.toString)
    } getOrElse df1.join(df2)
  }

  def toXml: Elem =
    <node type={ joinType.toString }>
      { condition map (c => <condition>{ c.toString }</condition>) toList }
    </node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("type" -> joinType.toString) ~
    ("condition" -> condition.map(_.toString))
}

/**
 * Join companion object.
 */
object Join {
  val tag = "join"

  def apply(): Join = apply(None, INNER)

  def apply(condition: Column): Join = apply(condition, INNER)

  def apply(condition: Column, joinType: JoinType): Join = apply(Some(condition.toString), joinType)

  def apply(condition: String): Join = apply(condition, INNER)

  def apply(condition: String, joinType: JoinType): Join = apply(Some(condition), joinType)

  def fromXml(xml: Node) = {
    val joinType = JoinType.withName(xml \ "@type" asString)
    val condition = (xml \ "condition" getAsString)

    apply(condition, joinType)
  }

  def fromJson(json: JValue) = {
    val joinType = JoinType.withName(json \ "type" asString)
    val condition = (json \ "condition" getAsString)

    apply(condition, joinType)
  }
}