import com.ignition.{JsonExport, XmlExport}
import org.apache.spark.sql.types.Decimal

/**
 * Helper functions.
 *
 * @author Vlad Orzhekhovskiy
 */
package object flow {

  /**
   * Creates a new UUID.
   */
  protected[flow] def newid = java.util.UUID.randomUUID.toString 

  /**
   * Constructs a java.sql.Date instance.
   */
  protected[flow] def javaDate(year: Int, month: Int, day: Int) = java.sql.Date.valueOf(s"$year-$month-$day")

  /**
   * Constructs a java.sql.Timestamp instance.
   */
  protected[flow] def javaTime(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int = 0) =
    java.sql.Timestamp.valueOf(s"$year-$month-$day $hour:$minute:$second")

  /**
   * Constructs a java.math.BigDecimal instance.
   */
  protected[flow] def javaBD(x: Double) = Decimal(x).toJavaBigDecimal

  /**
   * Constructs a java.math.BigDecimal instance.
   */
  protected[flow] def javaBD(str: String) = Decimal(str).toJavaBigDecimal

  /**
   * Prints out the XML representation of the data flow.
   */
  protected[flow] def printXml(flow: XmlExport) =
    println(new scala.xml.PrettyPrinter(80, 2).format(flow.toXml))

  /**
   * Prints out the JSON representation of the data flow.
   */
  protected[flow] def printJson(flow: JsonExport) =
    println(org.json4s.jackson.JsonMethods.pretty(flow.toJson))
}