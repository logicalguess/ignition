package flow

import com.ignition.{SparkHelper, frame}
import com.ignition.types._
import com.ignition.script.RichString
import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

object Flow1SparkPipeline extends App {
  import frame._

  var grid = DataGrid(string("id") ~ string("name") ~ int("weight") ~ date("dob"))

  grid = grid
    .addRow(newid, "john", 155, javaDate(1980, 5, 2))
    .addRow(newid, "jane", 190, javaDate(1982, 4, 25))
    .addRow(newid, "jake", 160, javaDate(1974, 11, 3))
    .addRow(newid, "josh", 120, javaDate(1995, 1, 10))


  implicit def FrameTransformerToSparkTransformer(ft: FrameTransformer): Transformer =
    new Transformer {

      override def transform(dataset: DataFrame) = ft.compute(dataset)

      override def copy(extra: ParamMap) = defaultCopy(extra)

      @DeveloperApi
      override def transformSchema(schema: StructType) = ft.outSchema

      override val uid: String = ""
    }


  val debug: FrameTransformer = DebugOutput()
  val formula: FrameTransformer = Formula("millenial" -> "YEAR(dob) >= 1982".mvel)
  val select: FrameTransformer = SelectValues() retain ("name", "weight", "millenial")
  val af: FrameTransformer = AddFields("predictor" -> "")

  grid --> formula --> select --> af --> debug

  val dt: FrameTransformer = debug

  val pipeline = new Pipeline()
    .setStages(Array(formula, select, af, debug))

  implicit protected val sc: SparkContext = SparkHelper.sparkContext
  implicit protected val ctx: SQLContext = SparkHelper.sqlContext
  implicit protected val runtime = new DefaultSparkRuntime(ctx)

  println(pipeline.transformSchema(grid.schema))

  val model = pipeline.fit(DataGrid(new StructType()).value)
  model.transform(grid.value)
}