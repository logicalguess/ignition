package flow

import com.ignition.frame.{BasicStats, SQLQuery, SelectValues, StringToLiteral}
import com.ignition.types.{RichStructType, double, fieldToRichStruct, int, string}
import com.ignition.{frame, stream}
import com.ignition.stream.{QueueInput, Foreach, Filter, StreamFlow}

object Flow2 extends App {

  val schema = string("name") ~ int("item") ~ double("score")
  val queue = QueueInput(schema).
    addRows(("john", 1, 65.0), ("john", 3, 78.0), ("jane", 2, 85.0)).
    addRows(("jane", 1, 46.0), ("jake", 4, 62.0), ("john", 3, 94.0))

  val sql = Foreach { SQLQuery("SELECT name, AVG(score) as score FROM input0 GROUP BY name") }
  val select = Foreach { SelectValues() rename ("score" -> "avg_score") }
  val stats = Foreach { BasicStats() groupBy "name" add ("item", frame.BasicAggregator.COUNT) }
  val filter = Filter($"avg_score" > 80)

  //filter.output foreachRDD { rdd => println(rdd) }

    val flow = StreamFlow {
    queue --> sql
    sql --> select --> filter
    queue --> stats
    (filter, stats)
  }

  println(flow.toXml)

  stream.Main.startStreamFlow(flow)

}
