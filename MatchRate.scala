package com.multiview.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import it.nerdammer.spark.hbase._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive._
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by anagarasan on 7/12/16.
  */
object MatchRate {

  def main(args: Array[String]): Unit = {

    val inputUrl = args(0)
    val inputDate = args(1)

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal1 = Calendar.getInstance()
    cal1.setTime(dateFormat.parse(inputDate))
    cal1.add(Calendar.DAY_OF_YEAR,-1)
    val prevDate = dateFormat.format(cal1.getTime)

    val dirMatchDly = s"/multiview/piwik_data_import_v1/matched_daily/$prevDate"

    val conf = new SparkConf().setAppName("Matchrate").set("spark.hbase.host", "10.85.9.159")
    val sc = new SparkContext(conf)
    val sqlC = new HiveContext(sc)

    import sqlC.implicits._

    val df = sqlC.read.format("parquet").load(dirMatchDly).distinct().select("mvid","pwid","pwsid")

    val pwMatched = df.select("pwid").distinct()
    val pwMatchCount = pwMatched.count().toFloat

    val logAction = sqlC.sql(s"""select idaction from piwik_log_action where instr(name, "$inputUrl") > 0""")
    val logVisits = sqlC.sql(s"""select idvisit, idvisitor from piwik_visits where from_unixtime(bigint(visit_last_action_time/1000),'yyyy-MM-dd') = '$prevDate'""")
    val logLink = sqlC.sql("select idvisit, idaction_url from piwik_log_link_visit_action where idaction_url is not null")

    val logDf01 = logAction.join(logLink,logAction("idaction") === logLink("idaction_url"))
    val logDf02 = logDf01.join(logVisits,logDf01("idvisit") === logVisits("idvisit"))
    val logDf03 = logDf02.select("idvisitor")

    val pwUnMatch = logDf03.rdd.subtract(pwMatched.rdd)
    val pwUnMatchCount = pwUnMatch.count().toFloat

    val totPwkCount =  pwMatchCount + pwUnMatchCount

    val hbaseRdd = sc.hbaseTable[(Option[String], Option[String],Option[String])]("cons_user_profile").select("inf/mv/userid", "inf/200/userid", "inf/300/userid").inColumnFamily("INFO")
    val userProfile  = hbaseRdd.mapPartitions(it => it.map(data => (data._1,data._2,data._3))).toDF("mvid","tdid","bmid")
    userProfile.registerTempTable("t1")

    val mvMatched = df.filter(df.col("pwsid") equalTo(1)).select("mvid").distinct()
    val mvMatchCount = mvMatched.count().toFloat
    mvMatched.registerTempTable("t2")

    val bmMatchCount = sqlC.sql("select t1.bmid from t1 join t2 on (t1.mvid = t2.mvid) where t1.bmid is not null and t1.bmid <> ''").count().toFloat
    val tdMatchCount = sqlC.sql("select t1.tdid from t1 join t2 on (t1.mvid = t2.mvid) where t1.tdid is not null and t1.tdid <> ''").count().toFloat

    val pwkRate = (pwMatchCount/totPwkCount) * 100
    val bmbRate = (bmMatchCount/mvMatchCount) * 100
    val tdRate  = (tdMatchCount/mvMatchCount) * 100

    val cookieMatchRate = sqlC.createDataFrame(Seq((prevDate,inputUrl,totPwkCount,pwMatchCount,pwUnMatchCount,pwkRate,bmbRate,tdRate))).toDF("Date","url","total_piwik_users","matched_piwik_users","unmatched_piwik_users","piwik_match_rate","bombora_match_rate","td_match_rate")
    cookieMatchRate.write.mode(SaveMode.Append).saveAsTable("cookie_matching_stats")

    sc.stop()
  }

}

