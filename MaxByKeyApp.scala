import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object MaxByKeyApp {
     def main (args:Array[String]) {
         val conf = new SparkConf().setAppName("MaxByKey Application")
         val sc = new SparkContext(conf)
         val OrdersRDD = sc.textFile("/user/cloudera/sqoop_import/orders")
         val OrderItemsRDD = sc.textFile("/user/cloudera/sqoop_import/order_items")
         val OrdersMap=OrdersRDD.map(rec => (rec.split(",")(0).toInt,rec.split(",")(1)))
         val OrderItemsMap=OrderItemsRDD.map(rec => (rec.split(",")(1).toInt,rec.split(",")(4).toFloat))
         val OrdersJoin = OrderItemsMap.join(OrdersMap)
         val OrdersJoinMap = OrdersJoin.map(rec => (rec._2._2 + "," + rec._1,rec._2._1)).reduceByKey((x,y) => x+y).map(rec => (rec._1.split(",")(0),(rec._1.split(",")(1).toInt,rec._2)))
         val MaxRevenueOrderPerDay = OrdersJoinMap.reduceByKey((x,y) => if (x._2 > y._2) x else y )
         MaxRevenueOrderPerDay.sortByKey().saveAsTextFile("file:///home/cloudera/Desktop/MaxRevenueOrderPerDay")
   }

}
