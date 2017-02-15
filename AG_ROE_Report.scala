package com.multiview.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by anagarasan on 2/15/17.
  */
object AG_ROE_Report {
  def main(args :Array[String]) {

    val inputDate = args(0)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal1 = Calendar.getInstance()
    cal1.setTime(dateFormat.parse(inputDate))
    cal1.add(Calendar.DAY_OF_YEAR, -1)
    val curr_date = dateFormat.format(cal1.getTime)

    val conf = new SparkConf().setAppName("AG_ROE_Report")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlC = new HiveContext(sc)

    val dirAdGroup = "/multiview/MaltDB/MV_AdGroup"
    val dirCampaign = "/multiview/MaltDB/MV_Campaign"
    val dirAdvertiser = "/multiview/MaltDB/MV_Advertiser"
    val dirRpt = s"/multiview/AdGroup_Report/Audience_ROE/dt=$curr_date/"

    val dfAdGroup = sqlC.read.option("delimiter", "|").format("com.databricks.spark.csv").option("inferSchema", "true").load(dirAdGroup).select("C0","C1","C2","C28","C47").toDF("AdGroupId","AdGroupName","CampaignId","AdvertiserId","AudienceId")
    val dfAdvertiser = sqlC.read.option("delimiter", "|").format("com.databricks.spark.csv").option("inferSchema", "true").load(dirAdvertiser).select("C0", "C1").toDF("AdvertiserId", "AdvertiserName")
    val dfCampaign = sqlC.read.option("delimiter", "|").format("com.databricks.spark.csv").option("inferSchema", "true").load(dirCampaign).select("C0","C2","C10").toDF("CampaignId", "CampaignName","VendorId")

    val dfRpt01 = dfAdGroup.filter(dfAdGroup("AudienceId").equalTo("null"))
    val dfRpt02 = dfCampaign.filter(dfCampaign("VendorId").equalTo(2))
    val dfRpt03 = dfAdvertiser.filter(dfAdvertiser("AdvertiserId").!==(8583))
    val dfRpt04 = dfRpt01.join(dfRpt02,"CampaignId").join(dfRpt03,"AdvertiserId").select("AdGroupId","AdGroupName","CampaignId","CampaignName","AdvertiserId","AdvertiserName")
    dfRpt04.orderBy("AdGroupId").repartition(1).write.format("com.databricks.spark.csv").option("delimiter", "|").save(dirRpt)

    sc.stop()
  }
}
