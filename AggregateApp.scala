import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object AggregateApp {
    def main (args: Array[String]) {
     val conf = new SparkConf().setAppName("Aggregate Application")
     val sc = new SparkContext(conf)
     val OrdersItems = sc.textFile("/user/cloudera/sqoop_import/order_items")
     val OrdersCount = OrdersItems.map(rec => rec.split(",")(1)).distinct().count()
     val OrdersValue = OrdersItems.map(rec => rec.split(",")(4).toFloat).reduce((accum,curr) => accum + curr)
     println ("================================================")
     println ("Total Orders count   : %s".format(OrdersCount))
     println ("Total Orders Revenue : %f".format(OrdersValue))
     println ("================================================")
     val OrdersMap = OrdersItems.map(rec => (rec.split(",")(1).toInt,rec.split(",")(4).toFloat))
     val revenuePerOrder = OrdersMap.reduceByKey((total,curr) => total + curr)
     val maxrevenueOrder = revenuePerOrder.reduce((max, curr) => if (max._2 > curr._2) max else curr )
     println ("================================================")
     println ("Max revenue Order_id : %s".format(maxrevenueOrder._1))
     println ("Max revenue          : %f".format(maxrevenueOrder._2))
   }
}
