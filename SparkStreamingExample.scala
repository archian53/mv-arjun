package com.multiview.utils

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by anagarasan on 9/1/16.
  */
object SparkStreamingExample {
  def main(args: Array[String]): Unit = {

    val host = "127.0.0.1"
    val port = 9999

    val conf = new SparkConf().setAppName("SparkStream").setMaster("local")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(10))
    ssc.checkpoint("/Users/anagarasan/Documents/tmp/ssc/produce")

    val lines = ssc.socketTextStream(host,port,StorageLevel.MEMORY_ONLY)
    val words = lines.flatMap(l=>l.split(" "))
    val wCount = words.map(w=>(w,1))
    val count = wCount.reduceByKey(_ + _)
    count.saveAsTextFiles("/Users/anagarasan/Documents/tmp/ssc/socketexample")

    ssc.start()
    ssc.awaitTermination()


    /*val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.map(x => x * x)
    println(result.collect().mkString(","))

    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = computeAvg(input)
    println(result)
    val avg = result._1 / result._2.toFloat
    println(avg)*/

    /*def computeAvg(input: RDD[Int]) = {
    input.aggregate((0, 0))((x, y) => {
      println("*****SeqOp*****")
      println(x)
      println(y)
      println("*****SeqOp*****")
      (x._1 + y, x._2 + 1)
    },
      (x, y) => {
        println("###CombineOp#####")
        println(x)
        println(y)
        println("###CombineOp#####")
        (x._1 + y._1, x._2 + y._2)
      })
  }*/

  }
}
