import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

object JsonFileReadApp {
    def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("JsonFileReading")
    val sc = new SparkContext(conf)
    val sqlContext =  new SQLContext(sc)
    val readJsonRDD=sqlContext.jsonFile("/user/cloudera/sqoop_import/departments_json")
    readJsonRDD.registerTempTable("departmentsjson")
    val tableJson=sqlContext.sql("select * from departmentsjson where department_id > 2 and department_id < 6")
    tableJson.collect().foreach(println)
    }
}

