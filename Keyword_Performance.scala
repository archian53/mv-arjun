package com.multiview.utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by anagarasan on 11/1/16.
  */
object Keyword_Performance {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Keyword_Performance").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlC = new SQLContext(sc)

    /*val dirKwMetric = "/Users/anagarasan/Documents/DMP/multiview/metrics/partner=simpli-fi/metric=organization_keyword_performance"
    val dirKwPerfRpt = "/Users/anagarasan/Documents/DMP/multiview/metrics/partner=simpli-fi/report=keyword"

    val kwMetric = sqlC.read.format("parquet").load(dirKwMetric)
    val kwReport = sqlC.read.format("parquet").load(dirKwPerfRpt)

    kwMetric.printSchema()
    kwMetric.show()

    kwReport.printSchema()
    kwReport.show()*/

    val ebayDir = "/Users/anagarasan/Documents/Text/ebay.csv"
    val ebayDS = sqlC.read.format("com.databricks.spark.csv").option("delimiter",",").option("inferSchema","true").load(ebayDir).toDF("Auction Id","bid","bidtime","bidder","bidderdate","openbid","price","item","dtl")

    //Task 1

    val auctionCount = ebayDS.select("Auction Id").distinct().count().toFloat

    println(s"Total Num of Auctions: $auctionCount")

    //Task 2

    val bpiDF = ebayDS.select("item","bid").groupBy("item").count()
    bpiDF.select("item","count").show()

    //Task 3

    ebayDS.groupBy("item").min("bid").show()
    ebayDS.groupBy("item").max("bid").show()
    ebayDS.groupBy("item").avg("bid").show()

    //Task 4

    val auction75 = ebayDS.filter(ebayDS("price")>75.0).select("Auction Id","item","price").distinct()
    auction75.orderBy("price").show()

    sc.stop()
  }
}
