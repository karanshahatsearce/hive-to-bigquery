// multi threading implemented working code.
import java.util.concurrent.Executors
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import com.hortonworks.hwc.HiveWarehouseSession
import com.hortonworks.hwc.HiveWarehouseSession._
import org.apache.spark.sql.functions._
import org.apache.spark._
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive
import java.sql.DriverManager
import org.apache.spark.sql.catalyst
import org.apache.hive.jdbc.HiveDriver
import org.apache.spark.util.Utils
import java.sql.SQLException
import java.util.Properties

class HiveTableFetcher(tableName: String) {
  println(s"Fetching data from table: $tableName")
}

object HiveTableFetcher {
  def main(args: Array[String]): Unit = {
    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(5))

    val spark = SparkSession.builder()
      .master("local")
      .config("spark.sql.hive.hiveserver2.jdbc.url", "jdbc:hive2://demo-hive-to-b1-m:10000")
      .enableHiveSupport()
      .appName("testing")
      .getOrCreate()

    val hive = HiveWarehouseSession.session(spark).build()
    hive.setDatabase("hivedemo")


      val list_table = hive.executeQuery("show tables")
  //  val All_table=list_table.collect().toList
      val All_table = list_table.collect().map(row => row.getString(0)).toList
      print("list of table"+All_table)

      val results = fetchTables(All_table)
       println(results.mkString("\n"))
  }


   def fetchTables(All_table : List[String]): List[String] = {

    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(5))
    val hive = HiveWarehouseSession.session(SparkSession.getActiveSession.get).build()
    All_table.map { tableName =>
      Future {
       //  val query = s"Select * from hivedemo.$tableName"
       //  val output = hive.executeQuery(query)
       //  output.show()
         val x = "select * from  hivedemo.%s where time_stamp > (current_timestamp() - INTERVAL '30' minute)"
         val r = x.format(tableName)
         print("--------------------------     tableName -------------------------------",tableName)

         val max_row_num="select row_number() over() as row_num from %s order by row_num desc limit 1"
         val max_row  =max_row_num.format(tableName)

         val get_max_Row=hive.executeQuery(max_row)
         val get_max=get_max_Row.collect().toList

        // print(max_row,"<---------------------------------------------------- max value--------------------------------->")

         val string_max_value = get_max.mkString(" ")
         val conv_str=string_max_value.replace("[","").replace("]","")

         println("conv_str",conv_str)
         val conv_int_max_rowcount=conv_str.toInt


          println(get_max,"get_max_Row")
       //   println(conv_int_max_rowcount,"------------>converted integer<-------------------------")


          var previous_row_number = conv_int_max_rowcount - 1
          val df = hive.executeQuery(r)

          df.show()
          val bucket="gs://hive-to-bigquery"
          df.write.format("bigquery").option("writeMethod","direct").mode("append").option("temporaryGcsBucket",bucket).save("applied-ai-practice00.hive_to_bigquery."+tableName)
          print("----------------------------------     Uploaded in Bigquery      ------------------------------------")
      s"Data from table $tableName fetched successfully"
      }
    }.map(Await.result(_, 3600.seconds))
  }
}

