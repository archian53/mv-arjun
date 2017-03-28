package examples.ingest.transform

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Archian53 on 3/27/17.
  */
object Pivot {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Pivot").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = sqlContext.read.format("com.databricks.spark.csv")
      .options(Map("path" -> "/Users/Archian53/Downloads/pivot.csv", "header" -> "true")).load()

    val df1 = df.withColumn("p", concat(lit("T"), $"dt"))
      .groupBy("tid", "tkr")
      .pivot("p")
      .agg(expr("first(stat)"))//.show()    // One Line Magic :)

    df1.show()

    val df2 = df1.na.fill("N/A",Seq("T201612"))
    df2.show()
    df.show()

   val newCol=when(col("C").equalTo("A"), "X").when(col("C").equalTo("B"), "Y").otherwise("Z")
    val df3 = df1.withColumn("C", newCol)

   
    sc.stop()

    /*+----+----+------+----+
      | tid| tkr|    dt|stat|
      +----+----+------+----+
      |ABCD|1234|201701|   I|
      |ABCD|1234|201702|   D|
      |ABCD|1234|201703|   C|
      |THYP|3456|201701|   O|
      |THYP|3456|201703|   I|
      |THYP| 345|201612|  AR|
      |XYZD|    |      |    |
      +----+----+------+----+*/


  }

}
