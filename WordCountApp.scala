import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object WordCountApp {
    def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("Word count application")
    val sc = new SparkContext(conf)
    val readRDD=sc.textFile("/user/cloudera/words.txt")
    val mapRDD=readRDD.flatMap(rec => rec.split(",")).map(rec => (rec,1))
    val reduceRDD=mapRDD.reduceByKey((x,y) => x+y)
    val sortRDD=reduceRDD.sortByKey()
    sortRDD.saveAsTextFile("/user/cloudera/words_count.txt")
    }
}
