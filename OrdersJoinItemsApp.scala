import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object OrdersJoinItemsApp {
    def main(args: Array[String]) {
        val conf=new SparkConf().setAppName("OrdersJoinItems App")
        val sc = new SparkContext(conf)
        val ordersRDD = sc.textFile("/user/cloudera/sqoop_import/orders")
        val orderItemsRDD = sc.textFile("/user/cloudera/sqoop_import/order_items")
        val ordersMap = ordersRDD.map(rec => (rec.split(",")(0).toInt,rec))
        val orderItemsMap = orderItemsRDD.map(rec => (rec.split(",")(1).toInt,rec))
        val joinRDD = orderItemsMap.join(ordersMap)
        val joinMap=joinRDD.map(t => (t._2._2.split(",")(1) + "," + t._1, t._2._1.split(",")(4).toFloat))
        val revenuePerOrderPerDay = joinMap.reduceByKey((x,y) => x+y)
        val totalOrdersPerDay=revenuePerOrderPerDay.map(t => t._1).distinct().map(t => (t.split(",")(0),1)).reduceByKey((x,y) => x+y)
        val totalRevenuePerDay=revenuePerOrderPerDay.map(t => (t._1.split(",")(0),t._2)).reduceByKey((x,y) => x+y)
        val Final=totalOrdersPerDay.join(totalRevenuePerDay).sortByKey()
        Final.saveAsTextFile("/user/cloudera/Spark_perDay_count")
}
}

        
