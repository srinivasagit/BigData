import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object DailyAvgApp {
     def main (args:Array[String]) {
         val conf = new SparkConf().setAppName("DailyAvg Application")
         val sc = new SparkContext(conf)
         val OrdersRDD = sc.textFile("/user/cloudera/sqoop_import/orders")
         val OrderItemsRDD = sc.textFile("/user/cloudera/sqoop_import/order_items")
         val OrdersMap=OrdersRDD.map(rec => (rec.split(",")(0).toInt,rec.split(",")(1)))
         val OrderItemsMap=OrderItemsRDD.map(rec => (rec.split(",")(1).toInt,rec.split(",")(4).toFloat))
         val OrdersJoin = OrderItemsMap.join(OrdersMap)
         val OrdersJoinMap = OrdersJoin.map(rec => (rec._2._2 + "," + rec._1,rec._2._1))
         val RevenuePerOrderPerDay = OrdersJoinMap.reduceByKey((x,y) => x+y).map(rec => (rec._1.split(",")(0),rec._2))
         val RevenuePerDay = RevenuePerOrderPerDay.combineByKey(value => (value,1), (accum:(Float,Int),v) => (accum._1 + v, accum._2 +1),(total:(Float,Int),curr:(Float,Int)) => (total._1 + curr._1, total._2 + curr._2))
         val AvgRevenuePerDay = RevenuePerDay.map(rec => (rec._1,rec._2._1/rec._2._2)).sortByKey().saveAsTextFile("file:///home/cloudera/Desktop/RevenuePerDay")
   }

}
