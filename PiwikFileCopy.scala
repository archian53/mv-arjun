package com.multiview.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * Created by anagarasan on 9/22/16.
  */
object PiwikFileCopy {

  def main(args: Array[String]): Unit = {

    val inputDate = args(0)

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal1 = Calendar.getInstance()
    cal1.setTime(dateFormat.parse(inputDate))
    cal1.add(Calendar.DAY_OF_YEAR, -1)
    val prevDate = dateFormat.format(cal1.getTime)

    val dirMatching = "/multiview/piwik_data_import_v1/matching-all-data"
    val dirMatchDly = s"/multiview/piwik_data_import_v1/matched_daily/$prevDate"

    val conf = new SparkConf().setAppName("PiwikFileCopy")
    val sc = new SparkContext(conf)
    val sqlC = new SQLContext(sc)

    val df = sqlC.read.format("parquet").load(dirMatching).toDF("mvid", "pwsid", "pwid")
    val oFile = df.distinct().select("mvid", "pwid", "pwsid")
    oFile.write.mode(SaveMode.Overwrite).save(dirMatchDly)

    sc.stop()
  }
}
