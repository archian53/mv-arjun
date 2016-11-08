package com.multiview.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by anagarasan on 8/31/16.
  */
object BomboraFile {

  def main(args:Array[String]):Unit = {


    val inputDate = "2016-10-27"

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal1 = Calendar.getInstance()
    cal1.setTime(dateFormat.parse(inputDate))
    cal1.add(Calendar.DAY_OF_YEAR,-1)
    val prevDate = dateFormat.format(cal1.getTime)
    val cal2 = Calendar.getInstance()
    cal2.setTime(dateFormat.parse(inputDate))
    cal2.add(Calendar.DAY_OF_YEAR,-2)
    val bmbDate = dateFormat.format(cal2.getTime)

    val dirBombora = s"/Users/anagarasan/Documents/DMP/multiview/bombora/day=$bmbDate/"

    print(dirBombora)

    val conf = new SparkConf().setAppName("BomboraFile").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlC = new SQLContext(sc)

    val bmbFile = sqlC.read.format("parquet").load(dirBombora)//.select("id","url","fp_id")

    //bmbFile.printSchema()

    //sftp://multiview@sftp.bombora.com/sftpdirectory/multiview/MultiView_Events_20161025.csv.gz

    /*val df = sqlC.read.
      format("com.springml.spark.sftp").
      option("host", "sftp.bombora.com").
      option("username", "multiview").
      option("password", "xbRzfDqgvY0x").
      option("fileType", "csv").
      option("inferSchema", "true").
      load("/sftpdirectory/multiview/MultiView_Events_20161025.csv.gz")

    df.printSchema()*/

    //bmbFile.rdd.saveAsTextFile("ftp://pftp-report-admin:fuJIA6NTYgsKSQeja4YQ@10.75.75.220/Report/test")



    val ipLong = bmbFile/*.select("ip")*/.filter(length(col("ip")) > 50)
    ipLong.show(1)

    //ipLong.write.format("com.databricks.spark.csv").option("header","true").option("codec", "org.apache.hadoop.io.compress.GzipCodec").save("test.csv.gz")

    /*ipLong.write.format("com.springml.spark.sftp").
      option("path","ftp://pftp-report-admin:fuJIA6NTYgsKSQeja4YQ@10.75.75.220").
      option("host", "10.75.75.220").
      option("port","21").
      option("username", "pftp-report-admin").
      option("password", "fuJIA6NTYgsKSQeja4YQ").
      option("fileType", "csv").save("/test/sample.csv")*/

    ipLong.rdd.saveAsTextFile("file:///home/anagarasan/Report")
    //ipLong.rdd.saveAsTextFile("ftp://pftp-report-admin:fuJIA6NTYgsKSQeja4YQ@10.75.75.220/Report/test")
    //ipLong.rdd.saveAsTextFile("ftp://anagarasan:Mv003424@10.75.75.220/Report/test")



    //ipLong.write.save(ftpDest)
    ipLong.show()
    //ipLong.rdd.saveAsTextFile("/Users/anagarasan/tmp")



    /*val bFile = bmbFile.filter(bmbFile.col("fp_id") isNotNull)

    val mvFile = bmbFile.filter(bmbFile.col("url") contains("multiview.com"))
    //val mpFile = bmbFile.filter(bmbFile.col("url") contains("museummarketplace.com"))

    val mvIdMV = mvFile.select(mvFile.col("fp_id")).distinct().count()
    val bmIdMV = mvFile.select(mvFile.col("id")).distinct().count()

    //val mvIdMP = mpFile.select(mpFile.col("fp_id")).distinct().count()
    //val bmIdMP = mpFile.select(mpFile.col("id")).distinct().count()


    print("multiview")
    print(mvIdMV)
    print("-----")
    print(bmIdMV)
    print("-----")
    print(mvIdMV.toFloat/bmIdMV.toFloat)

    print("museummarketplace")
    print(mvIdMP)
    print("-----")
    print(bmIdMP)
    print("-----")
    print(mvIdMP.toFloat/bmIdMP.toFloat)*/

  sc.stop()

  }

}
