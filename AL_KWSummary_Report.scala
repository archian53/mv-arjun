package com.multiview.utils

import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by anagarasan on 10/17/16.
  */
object AL_KWSummary_Report {
  def main(args: Array[String]) {

    val inputDate = args(0)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal1 = Calendar.getInstance()
    cal1.setTime(dateFormat.parse(inputDate))
    cal1.add(Calendar.DAY_OF_YEAR, -1)
    val curr_date = dateFormat.format(cal1.getTime)
    val dirKwdWrite = s"/multiview/AuditLog_Report/Keywords/Write/Write_$curr_date.csv"
    val dirAdGroup = "/multiview/MaltDB/MV_AdGroup"
    val dirCampaign = "/multiview/MaltDB/MV_Campaign"
    val dirAdvertiser = "/multiview/MaltDB/MV_Advertiser"
    val dirSummaryRpt = s"/multiview/AuditLog_Report/Keywords/Summary/dt=$curr_date/"

    val conf = new SparkConf().setAppName("AL_KWSummary_Report")
    val sc = new SparkContext(conf)
    val sqlC = new HiveContext(sc)

    val fc = new File(dirKwdWrite).exists().toString
    if (fc.contains("true")) {
      val kwdWriteSchema = StructType(Array(StructField("AUDIT ID", DataTypes.StringType, nullable = true),
                                              StructField("ADGROUP ID", DataTypes.StringType, nullable = true),
                                              StructField("USER ID", DataTypes.StringType, nullable = true),
                                              StructField("LOG DATE TIME", DataTypes.StringType, nullable = true),
                                              StructField("NAME", DataTypes.StringType, nullable = true),
                                              StructField("KEYWORD", DataTypes.StringType, nullable = true),
                                              StructField("NEW VALUE", DataTypes.StringType, nullable = true),
                                              StructField("OLD VALUE", DataTypes.StringType, nullable = true),
                                              StructField("STATE ID", DataTypes.StringType, nullable = true)))

      val kwSummary1 = sqlC.read.option("delimiter", ",").format("com.databricks.spark.csv").option("header", "true").schema(kwdWriteSchema).load(dirKwdWrite)
      val kwSummary2 = kwSummary1.groupBy(kwSummary1("ADGROUP ID")).count().orderBy(kwSummary1("ADGROUP ID"))
      val kwSummary3 = kwSummary1.join(kwSummary2, "ADGROUP ID").orderBy("ADGROUP ID")
      val dfAdGroup = sqlC.read.option("delimiter", "|").format("com.databricks.spark.csv").option("inferSchema", "true").load(dirAdGroup).select("C0", "C1", "C2").toDF("ADGROUP ID", "ADGROUP NAME", "CAMPAIGN ID")
      val dfAdvertiser = sqlC.read.option("delimiter", "|").format("com.databricks.spark.csv").option("inferSchema", "true").load(dirAdvertiser).select("C0", "C1").toDF("ADV ID", "ADV NAME")
      val dfCampaign = sqlC.read.option("delimiter", "|").format("com.databricks.spark.csv").option("inferSchema", "true").load(dirCampaign).select("C0", "C1", "C2").toDF("CAMPAIGN ID", "ADV ID", "CAMPAIGN NAME")
      val kwSummary4 = dfAdGroup.join(dfCampaign, "CAMPAIGN ID").join(dfAdvertiser, "ADV ID").orderBy("ADGROUP ID")
      val kwSummary5 = kwSummary3.join(kwSummary4, "ADGROUP ID")
      val kwSummary6 = kwSummary5.select("USER ID", "ADGROUP ID", "ADV NAME", "ADGROUP NAME", "CAMPAIGN NAME", "STATE ID", "count", "KEYWORD").toDF("USER ID", "ADGROUP ID", "COMPANY", "ADGROUP NAME", "CAMPAIGN NAME", "STATUS", "TOTAL KW EDITS", "MODIFIED VALUE").distinct()
      kwSummary6.orderBy("ADGROUP ID").repartition(1).write.format("com.databricks.spark.csv").option("delimiter", "|").save(dirSummaryRpt)
    }
  sc.stop()
  }
}
