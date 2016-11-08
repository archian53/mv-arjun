package com.multiview.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by anagarasan on 10/26/16.
  */
object FTP_Upload {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("FTP-Upload").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlC = new SQLContext(sc)

    val sourceDir = "Users/anagarasan/Documents/DMP/multiview/bombora/day=2016-10-25"

    val inputFile = sqlC.read.format("parquet").load(sourceDir)

    inputFile.write.format("com.springml.spark.sftp").
      option("host", "10.75.75.220").
      option("username", "pftp-report-admin").
      option("password", "fuJIA6NTYgsKSQeja4YQ").
      option("fileType", "csv").
      save("/Report/test/sample.csv")

    sc.stop()

  }
}
