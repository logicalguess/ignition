package com.ignition

import org.junit.runner.RunWith
import org.specs2.ScalaCheck
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FunctionSpec extends FlowSpecification with ScalaCheck {
  sequential

  trait FunctionStep {
    def toXml: scala.xml.Elem = ???
    def toJson: org.json4s.JValue = ???
  }

  class Mock extends FlowRuntime { val previewMode = false }

  implicit val rt = new Mock

  case class FunctionProducer[A](f: A) extends Producer[A, Mock] with FunctionStep {
    protected def compute(implicit runtime: Mock) = f
  }

  case class FunctionTransformer[A](f: A => A) extends Transformer[A, Mock] with FunctionStep {
    protected def compute(arg: A)(implicit runtime: Mock) = f(arg)
  }

  case class FunctionSplitter[A](f: A => A, outputCount: Int) extends Splitter[A, Mock] with FunctionStep {
    protected def compute(arg: A, index: Int)(implicit runtime: Mock) =
      f(arg)
  }

  case class FunctionMerger[A](f: Seq[A] => A, inputCount: Int, override val allInputsRequired: Boolean)
      extends Merger[A, Mock] with FunctionStep {
    protected def compute(args: IndexedSeq[A])(implicit runtime: Mock) =
      f(args)
  }

  case class FunctionModule[A](f: Seq[A] => A, inputCount: Int, outputCount: Int) extends Module[A, Mock] with FunctionStep {
    var computeCount: Int = 0
    protected def compute(args: IndexedSeq[A], index: Int)(implicit runtime: Mock) = {
      computeCount += 1
      f(args)
    }
  }

  def throwRT() = throw new RuntimeException("runtime")
  def throwWF() = throw ExecutionException("workflow")

  val concat: Seq[String] => String = args => args.filter(_ != null).mkString

  "Producer" should {
    "yield output" in prop { (s: String) =>
      val step = FunctionProducer(s)
      step.output === s
    }
    "fail for output(!=0)" in {
      val step = FunctionProducer("abc")
      step.output(1) must throwA[ExecutionException]
    }
    "wrap runtime error into workflow exception" in {
      val step = new FunctionProducer("abc") {
        override def compute(implicit runtime: Mock) = throwRT
      }
      step.output must throwA[ExecutionException](message = "Step computation failed")
    }
    "propagate workflow exception" in {
      val step = new FunctionProducer("abc") {
        override def compute(implicit runtime: Mock) = throwWF
      }
      step.output must throwA[ExecutionException](message = "workflow")
    }
  }

  "Transformer" should {
    "yield output" in prop { (s: String) =>
      val step0 = FunctionProducer[String](s)
      val step1 = FunctionTransformer[String](identity)
      step0 --> step1
      step1.output === s
      step1.value === s
    }
    "fail for output(!=0)" in {
      val step0 = FunctionProducer[String]("")
      val step1 = FunctionTransformer[String](identity)
      step0 --> step1
      step1.output(1) must throwA[ExecutionException]
    }
    "throw exception when not connected" in {
      val step = FunctionTransformer[String](identity)
      step.output must throwA[ExecutionException](message = "Input is not connected")
    }
  }

  "Splitter" should {
    "yield output" in prop { (s: String) =>
      val step0 = FunctionProducer[String](s)
      val step1 = FunctionSplitter[String](identity, 2)
      step0 --> step1
      step1.output(0) === s
      step1.output(1) === s
    }
    "fail for output(<0 or >= outputCount)" in {
      val step0 = FunctionProducer[String]("")
      val step1 = FunctionSplitter[String](identity, 2)
      step0 --> step1
      step1.output(-1) must throwA[ExecutionException]
      step1.output(2) must throwA[ExecutionException]
    }
    "throw exception when not connected" in {
      val step = FunctionSplitter[String](identity, 2)
      step.output must throwA[ExecutionException](message = "Input is not connected")
    }
  }

  "Merger" should {
    "yield output with mandatory inputs" in prop { (s1: String, s2: String, s3: String) =>
      val step1 = FunctionProducer(s1)
      val step2 = FunctionProducer(s2)
      val step3 = FunctionProducer(s3)
      val step4 = FunctionMerger[String](concat, 3, true)
      (step1, step2, step3) --> step4
      step4.output === s1 + s2 + s3
    }
    "yield output with optional inputs" in prop { (s1: String, s2: String) =>
      val step1 = FunctionProducer(s1)
      val step2 = FunctionProducer(s2)
      val step3 = FunctionMerger(concat, 3, false)
      step1 --> step3.in(0)
      step2 --> step3.in(2)
      step3.output === s1 + s2
    }
    "fail for output(!=0)" in {
      val step0 = FunctionProducer("")
      val step1 = FunctionMerger(concat, 1, false)
      step0 --> step1
      step1.output(1) must throwA[ExecutionException]
    }
    "fail when a mandatory input is not connected" in {
      val step1 = FunctionProducer("")
      val step2 = FunctionProducer("")
      val step3 = FunctionMerger(concat, 3, true)
      step1 --> step3.in(0)
      step2 --> step3.in(2)
      step3.output must throwA[ExecutionException](message = "Input1 is not connected")
    }
  }

  "Module" should {
    "yield output" in prop { (s1: String, s2: String) =>
      val step1 = FunctionProducer(s1)
      val step2 = FunctionProducer(s2)
      val step3 = FunctionModule[String](concat, 2, 2)
      (step1, step2) --> step3
      step3.output(0) === s1 + s2
      step3.output(1) === s1 + s2
    }
    "fail for output(<0 or >= outputCount)" in {
      val step0 = FunctionProducer[String]("")
      val step1 = FunctionModule[String](concat, 1, 1)
      step0 --> step1
      step1.output(-1) must throwA[ExecutionException]
      step1.output(1) must throwA[ExecutionException]
    }
  }

  "Step cache" should {
    "reuse computed values" in {
      val step0 = FunctionProducer("0")
      val step1 = FunctionProducer("1")
      val step2 = FunctionProducer("2")
      val step = FunctionModule[String](concat, 3, 3)
      (step0, step1, step2) --> step
      step.computeCount === 0
      step.output(0)
      step.computeCount === 1
      step.output(0)
      step.computeCount === 1
      step.output(2)
      step.computeCount === 2
      step.output(1)
      step.computeCount === 3
      step.output(2)
      step.computeCount === 3
    }
  }

  "Step connection operators" should {
    val p1 = FunctionProducer[String]("a")
    val t1 = FunctionTransformer[String](identity)
    val t2 = FunctionTransformer[String](identity)
    val t3 = FunctionTransformer[String](identity)
    val s1 = FunctionSplitter[String](identity, 2)
    val g1 = FunctionMerger(concat, 2, true)
    val m1 = FunctionModule[String](concat, 3, 3)

    "connect producers and transformers with `to`" in {
      (p1 to t1 to t2 to t3) === t3
      t1.inbound === p1
      t2.inbound === t1
      t3.inbound === t2
    }
    "connect producers and transformers with `-->`" in {
      (p1 --> t1 --> t2) === t2
      t1.inbound === p1
      t2.inbound === t1
      t3.inbound === t2
    }
    "connect multi-port steps with `out()` and `in()`" in {
      s1.out(0) to g1.in(1)
      g1.in(0).inbound == s1.out(0)
      (s1.out(1) to t1 to t2) === t2
      t1.inbound === s1.out(1)
      t2.inbound === t1
      p1 to t1 to m1.in(0)
      t1.inbound === p1
      m1.in(0).inbound === t1
      s1.out(1) --> (m1.in(1), t1)
      m1.in(1).inbound === s1.out(1)
      t1.inbound === s1.out(1)
    }
    "connect products with multi-input steps" in {
      (t1, t2) to m1
      m1.in(0).inbound === t1
      m1.in(1).inbound === t2
      (s1.out(1), t1) --> m1
      m1.in(0).inbound === s1.out(1)
      m1.in(1).inbound === t1
    }
    "connect muti-output steps with products" in {
      s1 to (t1, t2)
      t1.inbound === s1.out(0)
      t2.inbound === s1.out(1)
      s1 --> (t1, t2)
      t1.inbound === s1.out(0)
      t2.inbound === s1.out(1)
      m1 --> (t1, g1.in(1))
      t1.inbound === m1.out(0)
      g1.in(1).inbound === m1.out(1)
    }
  }
}