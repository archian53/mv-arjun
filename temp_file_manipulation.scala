package com.multiview.utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by anagarasan on 10/5/16.
  */
object MetricsLocation {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Location").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlC = new SQLContext(sc)


    /*val device08 = "/Users/anagarasan/Desktop/organization_device_type_performance_company_5629_20160801-20160831.csv"
    val df01 = sqlC.read.format("com.databricks.spark.csv").option("inferSchema","true").option("header","true").load(device08)
    df01.printSchema()
    val df02 = df01.select("Campaign Id","Device Type","Date")
    df02.show()
    df02.distinct().show()
    //println(df02.count())
    //println(df02.distinct().count())
    //println(df01.count())
    //println(df01.distinct().count()) */

    /*val url = "https://secure.dialogtech.com/ibp_api.php?api_key=edb226432e04fc505c3785ea0b11eb131fdc2d4cc8b7b0a814f79e402bce94a6&action=report.call_detail_csv&start_date=20170126&end_date=20170126&lookup=1"
    //val url = "https://secure.dialogtech.com/ibp_api.php?api_key=edb226432e04fc505c3785ea0b11eb131fdc2d4cc8b7b0a814f79e402bce94a6&action=report.call_detail&start_date=20170131&end_date=20170131&call_type_filter=All"
    val text1 = scala.io.Source.fromURL(url).mkString

    val rep = text1.split("\n")

    val rp = sc.parallelize(rep)

    val f = new File("/Users/anagarasan/Downloads/callreport.csv")
    val bw = new BufferedWriter(new FileWriter(f))
    bw.write(text1)
    bw.close()

    val report = sqlC.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("/Users/anagarasan/Downloads/callreport.csv")
    report.printSchema()
    report.show(2)*/


    val input = "/Users/anagarasan/Downloads/20170130.gz.parquet"
    val df = sqlC.read.format("parquet").load(input)
    df.persist(StorageLevel.MEMORY_ONLY)
    println(df.count())
    println(df.distinct().count())

    sc.stop()
  }
}
