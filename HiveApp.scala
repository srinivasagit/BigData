import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext


object HiveApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Hive Application")
    val sc = new SparkContext(conf)
    val sqlContext=new HiveContext(sc)
    val tableDF = sqlContext.sql("select c.category_id,c.category_name,d.department_name from sqoop_import.categories c join sqoop_import.departments d where c.category_department_id=d.department_id")
    val FilterDF=tableDF.filter(tableDF("category_id") > 10)
    val FinalDF=FilterDF.collect()
    FinalDF.foreach(println)
  }
}

