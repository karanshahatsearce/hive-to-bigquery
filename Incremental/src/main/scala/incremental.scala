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

class MyInc extends Thread
{
        override def run()
        {
                println("Thread " + Thread.currentThread().getName() +  " is running.")
        }
}
// Creating object
object Incremental_Schema
{        def main(args: Array[String])
        {

       val spark =SparkSession.builder().master("local").config("spark.sql.hive.hiveserver2.jdbc.url","jdbc:hive2://demo-hive-to-b2-m:10000").enableHiveSupport().appName("testing").getOrCreate()
       val hive = HiveWarehouseSession.session(spark).build()
       hive.setDatabase("hivedemo")
       val output = hive.executeQuery("select * from products")
       output.show()
       val list_table = hive.executeQuery("show tables")
       val All_table=list_table.collect().toList
       print("list of table"+All_table)
           for (name <- All_table)
                      {
                          println(name)
                          val string_row=name.mkString(" ")
                          var th = new MyInc()
                          th.setName(string_row.toString())
                          th.start()

                          val x = "select * from  %s where time_stamp > (current_timestamp() - INTERVAL '30' minute)"
                          val r = x.format(string_row)

                          val max_row_num="select row_number() over() as row_num from %s order by row_num desc limit 1"
                          val max_row  =max_row_num.format(string_row)

                          val get_max_Row=hive.executeQuery(max_row)
                          val get_max=get_max_Row.collect().toList
                          print(max_row,"<---------------------------------------------------- max value--------------------------------->")
                          val string_max_value = get_max.mkString(" ")
                          val conv_str=string_max_value.replace("[","").replace("]","")

                          println("conv_str",conv_str)
                          val conv_int_max_rowcount=conv_str.toInt


                          println(get_max,"get_max_Row")
                          println(conv_int_max_rowcount,"------------>converted integer<-------------------------")


                          var previous_row_number = conv_int_max_rowcount - 1
print(previous_row_number,"==================================> previous_row_number========================================")
                          val df = hive.executeQuery(r)
df.show()
                          val bucket="gs://hive-to-bigquery"
df.write.format("bigquery").option("writeMethod","direct").mode("append").option("temporaryGcsBucket",bucket).save("applied-ai-practice00.hive_to_bigquery."+string_row)
                          println("------------------------------------------------------>successfully loaded data in bigquery<-------------------------------------->")


                }
        }
}



