package flow

import com.ignition.frame
import com.ignition.script.RichString
import com.ignition.types._

object Flow1 extends App {
  import frame._

  var grid = DataGrid(string("id") ~ string("name") ~ int("weight") ~ date("dob"))

  def createFlow(grid: DataGrid): FrameFlow = FrameFlow {

    val debugA: FrameTransformer = DebugOutput()
    grid --> debugA

    val formula: FrameTransformer = Formula("millenial" -> "YEAR(dob) >= 1982".mvel)
    val select = SelectValues() retain ("name", "weight", "millenial")
    val af = AddFields("predictor" -> "")
    val debugB: FrameTransformer = DebugOutput()
    grid --> formula --> select --> af --> debugB

    (debugA, debugB)
  }

  println(createFlow(grid).toXml)

  grid = grid
    .addRow(newid, "john", 155, javaDate(1980, 5, 2))
    .addRow(newid, "jane", 190, javaDate(1982, 4, 25))
    .addRow(newid, "jake", 160, javaDate(1974, 11, 3))
    .addRow(newid, "josh", 120, javaDate(1995, 1, 10))

  frame.Main.runFrameFlow(createFlow(grid))
}