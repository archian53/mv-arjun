package com.multiview.utils

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by anagarasan on 12/8/16.
  */
object MLExample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Flight Data").setMaster("local")
    val sc = new SparkContext(conf)

    // define the Flight Schema
    case class Flight(dofM: String, dofW: String, carrier: String, tailnum: String, flnum: Int, org_id: String, origin: String, dest_id: String, dest: String, crsdeptime: Double, deptime: Double, depdelaymins: Double, crsarrtime: Double, arrtime: Double, arrdelay: Double, crselapsedtime: Double, dist: Int)

    // function to parse input into Flight class
    def parseFlight(str: String): Flight = {
      val line = str.split(",")
      Flight(line(0), line(1), line(2), line(3), line(4).toInt, line(5), line(6), line(7), line(8), line(9).toDouble, line(10).toDouble, line(11).toDouble, line(12).toDouble, line(13).toDouble, line(14).toDouble, line(15).toDouble, line(16).toInt)
    }

    val txtRdd = sc.textFile("/Users/anagarasan/Downloads/fldata012014.csv")
    val fltRdd = txtRdd.map(parseFlight).cache()
    fltRdd.take(5).foreach(println)

    var carrierMap: Map[String,Int] = Map()
    var index =0
    fltRdd.map(fl=>fl.carrier).distinct().collect().foreach(x=>{carrierMap+=(x->index);index+=1})
    println(carrierMap.toString())

    var originMap:Map[String,Int] = Map()
    var index1 = 0
    fltRdd.map(fl=>fl.origin).distinct().collect().foreach(x=>{originMap+=(x->index1);index1+=1})
    println(originMap.toString())

    var destMap:Map[String,Int] = Map()
    var index2 = 0
    fltRdd.map(fl=>fl.dest).distinct().collect().foreach(x=>{destMap+=(x->index2);index2+=1})
    println(destMap.toString())

    //- Defining the features array
    val mlprep = fltRdd.map(fl => {
      val monthday = fl.dofM.toInt - 1 // category
      val weekday = fl.dofW.toInt - 1 // category
      val crsdeptime1 = fl.crsdeptime.toInt
      val crsarrtime1 = fl.crsarrtime.toInt
      val carrier1 = carrierMap(fl.carrier) // category
      val crselapsedtime1 = fl.crselapsedtime//.toDouble
      val origin1 = originMap(fl.origin) // category
      val dest1 = destMap(fl.dest) // category
      val delayed = if (fl.depdelaymins > 40) 1.0 else 0.0
      Array(delayed.toDouble, monthday.toDouble, weekday.toDouble, crsdeptime1.toDouble, crsarrtime1.toDouble, carrier1.toDouble, crselapsedtime1.toDouble, origin1.toDouble, dest1.toDouble)
    })
   mlprep.take(1).foreach(i=>i.foreach(println))

    //Making LabeledPoint of features - this is the training data for the model

    val mldata = mlprep.map(x=>LabeledPoint(x(0),Vectors.dense(x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8))))
    mldata.take(5).foreach(println)

    // %85 not delayed flights
    val mldata0 = mldata.filter(x=>x.label==0).randomSplit(Array(0.85,0.15))(1)
    // %100 delayed flights
    val mldata1 = mldata.filter(x=>x.label!=0)
    val mldata2 = mldata0++mldata1

    //training + test data split

    val splits = mldata2.randomSplit(Array(0.7,0.3))
    val (train,test) = (splits(0),splits(1))

    train.take(5).foreach(println)
    test.take(5).foreach(println)



    sc.stop()
  }
}
