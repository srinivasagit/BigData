import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object SequenceFileApp {
   def main (args:  Array[String]) {
      val conf= new SparkConf().setAppName("SequenceFileApp")
      val sc  = new SparkContext(conf)
      val fileRDD=sc.textFile("/user/cloudera/sqoop_import/departments")
      val FinalRDD=fileRDD.map(rec => (rec.split(",")(0),rec))
      FinalRDD.saveAsSequenceFile("/user/cloudera/sqoop_import/departments_seq")
   }
}



