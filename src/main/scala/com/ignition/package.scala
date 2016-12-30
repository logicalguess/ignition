package com

import scala.util.Try

/**
 * Ignition implicits and helper functions.
 *
 * @author Vlad Orzhekhovskiy
 */
package object ignition {

  val STEPS_SERIALIZABLE = "step.serializable"

  private type CSrc[T, R <: FlowRuntime] = ConnectionSource[T, R]

  /* implicits for connecting tuples of ConnectionSource to a multi-input step */

  implicit class CSource2[T, R <: FlowRuntime](val tuple: Product2[_, _]) extends AnyVal {
    def to(tgt: MultiInputStep[T, R]): tgt.type = {
      tuple._1.asInstanceOf[CSrc[T, R]] to tgt.in(0)
      tuple._2.asInstanceOf[CSrc[T, R]] to tgt.in(1)
      tgt
    }
    def -->(tgt: MultiInputStep[T, R]): tgt.type = to(tgt)
  }

  implicit class CSource3[T, R <: FlowRuntime](val tuple: Product3[_, _, _]) extends AnyVal {
    def to(tgt: MultiInputStep[T, R]): tgt.type = {
      //(tuple._1.asInstanceOf[CSrc[T, R]], tuple._2.asInstanceOf[CSrc[T, R]]) to tgt
      tuple._1.asInstanceOf[CSrc[T, R]] to tgt.in(0)
      tuple._2.asInstanceOf[CSrc[T, R]] to tgt.in(1)
      tuple._3.asInstanceOf[CSrc[T, R]] to tgt.in(2)
      tgt
    }
    def -->(tgt: MultiInputStep[T, R]): tgt.type = to(tgt)
  }

  implicit class CSource4[T, R <: FlowRuntime](val tuple: Product4[CSrc[T, R], CSrc[T, R], CSrc[T, R], CSrc[T, R]]) extends AnyVal {
    def to(tgt: MultiInputStep[T, R]): tgt.type = {
      (tuple._1, tuple._2, tuple._3) to tgt
      tuple._4 to tgt.in(3)
      tgt
    }
    def -->(tgt: MultiInputStep[T, R]): tgt.type = to(tgt)
  }

  implicit class CSource5[T, R <: FlowRuntime](val tuple: Product5[CSrc[T, R], CSrc[T, R], CSrc[T, R], CSrc[T, R], CSrc[T, R]]) extends AnyVal {
    def to(tgt: MultiInputStep[T, R]): tgt.type = {
      (tuple._1, tuple._2, tuple._3, tuple._4) to tgt
      tuple._5 to tgt.in(4)
      tgt
    }
    def -->(tgt: MultiInputStep[T, R]): tgt.type = to(tgt)
  }

  /**
   * Converts a single value to a Product of size 1 for consistency in some API calls.
   */
  implicit def value2tuple[U](x: U): Tuple1[U] = Tuple1(x)

  /**
   * Returns the outbound ports of a step.
   * Having this private convenience function rather than making Step more generic and
   * less type safe.
   */
  def outs[T, R <: FlowRuntime](step: Step[T, R]): Seq[ConnectionSource[T, R]] = step match {
    case x if x.isInstanceOf[SingleOutputStep[T, R]] => List(x.asInstanceOf[SingleOutputStep[T, R]])
    case x if x.isInstanceOf[MultiOutputStep[T, R]] => x.asInstanceOf[MultiOutputStep[T, R]].out
    case _ => Nil
  }

  /**
   * Returns the inbounds ports of a step.
   * Having this private convenience function rather than making Step more generic and
   * less type safe.
   */
  def ins[T, R <: FlowRuntime](step: Step[T, R]): Seq[ConnectionTarget[T, R]] = step match {
    case x if x.isInstanceOf[SingleInputStep[T, R]] => List(x.asInstanceOf[SingleInputStep[T, R]])
    case x if x.isInstanceOf[MultiInputStep[T, R]] => x.asInstanceOf[MultiInputStep[T, R]].in
    case _ => Nil
  }

  /**
   * Helper class providing a simple syntax to add side effects to the returned value:
   *
   * {{{
   * def square(x: Int) = {
   * 		x * x
   * } having (r => println "returned: " + r)
   * }}}
   *
   * or simplified
   *
   * {{{
   * def square(x: Int) = (x * x) having println
   * }}}
   */
  final implicit class Having[A](val result: A) extends AnyVal {
    def having(body: A => Unit): A = {
      body(result)
      result
    }
    def having(body: => Unit): A = {
      body
      result
    }
  }

  /**
   * Returns an instance of the specified class. Object class names have "$" character
   * at the end.
   */
  def getClassInstance[T](className: String) =
    Try(Class.forName(className).asInstanceOf[Class[T]]) map instantiate get

  /**
   * If the argument is a class, create and returns a new instance of it; if it is
   * an object, returns the object.
   */
  def instantiate[T](clazz: Class[T]) = Try {
    clazz.getField("MODULE$").get(null).asInstanceOf[T]
  } getOrElse clazz.newInstance
}