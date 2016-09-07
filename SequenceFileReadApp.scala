import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.io._

object SequenceFileReadApp {
   def main (args: Array[String]){
   val conf = new SparkConf().setAppName("SequenceFileReading")
   val sc = new SparkContext(conf)
   val readRDD=sc.sequenceFile("/user/cloudera/sqoop_import/departments_seq",classOf[IntWritable],classOf[Text])
   readRDD.map(rec => rec.toString()).collect().foreach(println)
   }
}
