package com.multiview.utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by anagarasan on 10/5/16.
  */
object MetricsLocation {
  def main(args: Array[String]): Unit = {

    val curr_date = "2016-09-27"
    val dirLogMaster = s"/Users/anagarasan/Documents/DMP/multiview/AuditLog/source/dt=$curr_date"

    val conf = new SparkConf().setAppName("Location").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlC = new SQLContext(sc)
    val logSchema = StructType(Array(StructField("Id", DataTypes.IntegerType, nullable = true),
      StructField("UserName", DataTypes.StringType, nullable = true),
      StructField("AuditTypeId", DataTypes.IntegerType, nullable = true),
      StructField("Source", DataTypes.StringType, nullable = true),
      StructField("Summary", DataTypes.StringType, nullable = true),
      StructField("Details", DataTypes.StringType, nullable = true),
      StructField("LogDateTime", DataTypes.StringType, nullable = true),
      StructField("WhoId", DataTypes.StringType, nullable = true)))

    val df0 = sqlC.read.option("delimiter", "|").format("com.databricks.spark.csv").schema(logSchema).load(dirLogMaster)
    val logMaster = df0.select(df0("Id"),df0("UserName"),df0("AuditTypeId"),df0("Source"),df0("Summary"),df0("LogDateTime"),df0("WhoId"))
    //logMaster.write.save(s"$dirTarget/logmaster/dt=$curr_date")

    val customConcat1 = (Id: Int, Details: String) => {s""" { "Id" : $Id, "Details":$Details } """}
    val toJson1 = udf(customConcat1)
    val customConcat2 = (eid: Int, n: String, o: String) => {s""" { "eid" : $eid, "DetailsN":$n, "DetailsO":$o } """}
    val toJson2 = udf(customConcat2)

    //Processing ADGROUP Records

    val adGroup = df0.filter(df0("Source") === "MV_AdGroup")
    if (adGroup.count() >1) {
      val ds1 = adGroup.select(toJson1(adGroup("Id"), adGroup("Details")) as "jsonKey").rdd.map(Row => Row.getString(0))
      val jsonAdGroup = sqlC.read.json(ds1)
      //jsonAdGroup.select("Id","Details.entityId","Details.entityType","Details.parentId","Details.fields.fieldName","Details.fields.newValue","Details.fields.oldValue").write.save(s"$dirTarget/logadgroup/dt=$curr_date")

      val ag01 = jsonAdGroup.select("Id","Details.fields").select(col("Id").as("eid"),org.apache.spark.sql.functions.explode(col("fields")).as("non"))
      val ag02 = ag01.select(col("eid"),col("non").getField("fieldName").as("fieldName"),col("non").getField("newValue").as("newValue"),col("non").getField("oldValue").as("oldValue"))

      //Processing ADGROUP - adGroupToTDDeviceTypes

      val agToTDDeviceTypes01 = ag02.filter(col("fieldName")==="adGroupToTDDeviceTypes")
      if (agToTDDeviceTypes01.count()>1) {
        val agToTDDeviceTypes02 = agToTDDeviceTypes01.select(toJson2(ag02("eid"), ag02("newValue"), ag02("oldValue"))).rdd.map(Row => Row.getString(0))
        val agTTDDTN = sqlC.read.json(agToTDDeviceTypes02).select("eid","DetailsN").select(col("eid").as("eid"),org.apache.spark.sql.functions.explode(col("DetailsN")).as("n"))

        val agTTDTOSchema = sqlC.read.json(agToTDDeviceTypes02).select("DetailsO").schema.toString()


        sqlC.read.json(agToTDDeviceTypes02).select("DetailsO").printSchema()

        print(agTTDTOSchema)

        if (agTTDTOSchema.contains("ArrayType")) {

          val agTTDDTO = sqlC.read.json(agToTDDeviceTypes02).select("eid","DetailsO").select(col("eid").as("eid"),org.apache.spark.sql.functions.explode(col("DetailsO")).as("o"))
          agTTDDTO.select(col("eid").as("eid")
            ,col("o").getField("bidAdjustment").as("OldBidAdjustment")
            ,col("o").getField("tradeDeskDeviceTypeId").as("OldTDDeviceTypeId")).printSchema()//.write.save(s"$dirTarget/adgroup/tddevicetypes/old/dt=$curr_date")}

        agTTDDTN.select(col("eid").as("eid")
          ,col("n").getField("bidAdjustment").as("NewBidAdjustment")
          ,col("n").getField("tradeDeskDeviceTypeId").as("NewTDDeviceTypeId")).printSchema()//.write.save(s"$dirTarget/adgroup/tddevicetypes/new/dt=$curr_date")

      }}}

    sc.stop()
  }
}
