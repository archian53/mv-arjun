package com.multiview.utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by anagarasan on 1/4/17.
  * http://homepage.cs.latrobe.edu.au/zhe/ZhenHeSparkRDDAPIExamples.html#keyBy
  */
object Transformations {
  def main(args : Array[String]):Unit = {

    val conf = new SparkConf().setAppName("Transformations").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlC = new SQLContext(sc)

    //val bn = sqlC.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("/Users/anagarasan/Downloads/baby_names.csv")
    val bn = sc.textFile("/Users/anagarasan/Downloads/baby_names.csv")
    bn.take(5).foreach(println)
    //map
    val r = bn.map(_.split(","))
    r.take(5).foreach(_.foreach(println))

    //flatMap
    val fMap = sc.parallelize(List(1,2,3)).flatMap(x=>List(x,x,x))
    val nMap = sc.parallelize(List(1,2,3)).map(x=>List(x,x,x))
    fMap.collect.foreach(println)
    nMap.collect.foreach(println)

    //filter
    val fltr = bn.filter(_.contains("girl"))
    fltr.take(5).foreach(println)

    //mapPartitions
    val p3 = sc.parallelize(1 to 9,5)
    val rmp = p3.mapPartitions(x=>List(x.next).iterator)
    rmp.foreach(println)
    val p1 = sc.parallelize(1 to 9,9)
    val dmp = p1.mapPartitions(x=>List(x.next).iterator)
    dmp.foreach(println)

    //mapPartitionsWithIndex
    val imp1 = p1.mapPartitionsWithIndex((x:Int,y:Iterator[Int])=>y.toList.map(z=>x+"|"+z).iterator)
    imp1.foreach(println)
    val imp3 = p3.mapPartitionsWithIndex((x:Int,y:Iterator[Int])=>y.toList.map(z=>x+"|"+z).iterator)
    imp3.foreach(println)

    //sample
    val smp = p1.sample(true,.4)
    smp.foreach(println)
    println("smp count "+smp.count)

    // Union + Intersection + Distinct + Subtract
    val g1 = sc.parallelize(5 to 10)
    val g2 = sc.parallelize(7 to 15)
    val gu = g1.union(g2)
    val gi = g1.intersection(g2)
    val gd = g1.union(g2).distinct
    val gs = gu.subtract(gi)
    gu.collect.foreach(println)
    gi.collect.foreach(println)
    gd.collect.foreach(println)
    gs.collect.foreach(println)

    //groupByKey

    val namesToCounties = r.map(n=>(n(1),n(2))).groupByKey
    namesToCounties.take(5).foreach(println)
    val namesFil = r.filter(_.contains("ARJUN"))
    println("Filter Count ="+namesFil.count)

    //reduceByKey + sortbyKey + aggregateByKey
    val frows = bn.filter(l=> !l.contains("Count")).map(_.split(","))
    val nameCount = frows.map(n=>(n(1),n(4).toInt)).reduceByKey(_+_)
    val nameCount1 = frows.map(n=>(n(1),n(4))).aggregateByKey(0)((k,v)=>v.toInt+k,(v,k)=>k+v)
    nameCount.take(5).foreach(println)
    nameCount1.take(5).foreach(println)
    nameCount.sortByKey().take(5).foreach(println)
    nameCount.sortByKey(false).take(5).foreach(println)

    //Action - CountByKey
    val nameCount2 = frows.map(n=>(n(1),1))
    nameCount2.take(5).foreach(println)
    nameCount2.countByKey().take(10).foreach(println)


    //joins
    val n1 = sc.parallelize(List("abe","abby","apple")).map(a=>(a,1))
    val n2 = sc.parallelize(List("apple","beatty","beatrice")).map(a=>(a,1))
    val nj = n1.join(n2)
    val loj = n1.leftOuterJoin(n2)
    val roj = n1.rightOuterJoin(n2)
    nj.foreach(println)
    loj.foreach(println)
    roj.foreach(println)


  }

}
