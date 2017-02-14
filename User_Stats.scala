package com.multiview.utils

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by anagarasan on 12/12/16.
  */
object User_Stats {
  def main(args:Array[String]):Unit = {

    val conf = new SparkConf().setAppName("User_Stats")//.setMaster("local")
    val sc = new SparkContext(conf)
    val SqlC = new HiveContext(sc)
    /*
    case class User(id:String,mvid:String,ts:String)
    val uProf = SqlC.read.format("parquet").load("/multiview/piwik_data_import_v1/matched_daily/2016-09-")
    uProf.repartition(1).rdd.saveAsTextFile("/user/anagarasan/201609/")
    uProf.write.saveAsTable("user_site_txt6")


    val schema = StructType(Array(StructField("acc", DataTypes.IntegerType, nullable = true),
      StructField("Company", DataTypes.StringType, nullable = true),
      StructField("pwsid", DataTypes.IntegerType, nullable = true),
      StructField("url", DataTypes.StringType, nullable = true)))

    val idSite = SqlC.read.format("com.databricks.spark.csv").option("header","true").schema(schema).load("/Users/anagarasan/Downloads/CustomerPortal_Piwik_Sites.csv")
    val userTable = idSite./*select("pwsid","url").*/dropDuplicates().orderBy("pwsid")
    val t2 = userTable.groupBy("pwsid").max("acc").orderBy(asc("pwsid")).toDF("pwsid","acc")
    val t3 = t2.join(idSite,t2.col("pwsid")===idSite.col("pwsid")and(t2.col("acc")===idSite.col("acc"))).select(t2.col("pwsid"),t2.col("acc"),idSite.col("Company"),idSite.col("url"))
    t3.repartition(1).write.format("com.databricks.spark.csv").option("delimiter","|").save("/Users/anagarasan/Downloads/cust")*/

    val df01 = SqlC.sql("select mvid,pwid,tdid,bmid,dt from user_id2")
    df01.repartition(1).write.format("com.databricks.spark.csv").option("header","true").option("delimiter","|").save("/user/anagarasan/userid2")

    sc.stop()
  }

}
