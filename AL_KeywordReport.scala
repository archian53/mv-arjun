package com.multiview.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by anagarasan on 10/19/16.
  */
object AL_KeywordReport {
  def main(args: Array[String]) {
    val inputDate = args(0)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal1 = Calendar.getInstance()
    cal1.setTime(dateFormat.parse(inputDate))
    cal1.add(Calendar.DAY_OF_YEAR, -1)
    val curr_date = dateFormat.format(cal1.getTime)
    val dirLogMaster = s"/multiview/auditlog/source/dt=$curr_date"
    //val dirLogMaster = s"/Users/anagarasan/Documents/DMP/multiview/auditlog/source/dt=$curr_date"
    val dirKwRpt = s"/multiview/AuditLog_Report/Keywords/Write/dt=$curr_date"
    val dirKcRpt = s"/multiview/AuditLog_Report/Keywords/Create/dt=$curr_date"
    val dirMVKwd = "/multiview/MaltDB/MV_Keyword"
    val dirAdGroup = "/multiview/MaltDB/MV_AdGroup"
    val dirCampaign = "/multiview/MaltDB/MV_Campaign"
    val dirAdvertiser = "/multiview/MaltDB/MV_Advertiser"
    val dirSummaryRpt = s"/multiview/AuditLog_Report/Keywords/Summary/dt=$curr_date/"

    val conf = new SparkConf().setAppName("AL_KeywordReport")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlC = new HiveContext(sc)

    val auditSchema = StructType(Array(StructField("Id", DataTypes.IntegerType, nullable = true),
      StructField("UserName", DataTypes.StringType, nullable = true),
      StructField("AuditTypeId", DataTypes.IntegerType, nullable = true),
      StructField("Source", DataTypes.StringType, nullable = true),
      StructField("Summary", DataTypes.StringType, nullable = true),
      StructField("Details", DataTypes.StringType, nullable = true),
      StructField("LogDateTime", DataTypes.StringType, nullable = true),
      StructField("WhoId", DataTypes.StringType, nullable = true)))

    val kwdSchema = StructType(Array(StructField("Id", DataTypes.LongType, nullable = true),
      StructField("Name", DataTypes.StringType, nullable = true),
      StructField("BidAmount", DataTypes.StringType, nullable = true),
      StructField("StateId", DataTypes.StringType, nullable = true),
      StructField("AdGroupId", DataTypes.StringType, nullable = true),
      StructField("CreatedDateTime", DataTypes.StringType, nullable = true),
      StructField("LastModifiedDateTime", DataTypes.StringType, nullable = true)))

    val logMaster = sqlC.read.option("delimiter", "|").format("com.databricks.spark.csv").schema(auditSchema).load(dirLogMaster).persist()
    val kwdMaster = sqlC.read.option("delimiter", "|").option("nullValue", "null").format("com.databricks.spark.csv").schema(kwdSchema).load(dirMVKwd).persist()
    val kwdMaster1 = kwdMaster.select("Id","Name","StateId","AdGroupId").persist()
    val customConcat1 = (Id: Int, Details: String) => {s""" { "Id" : $Id, "Details":$Details } """}
    val toJson1 = udf(customConcat1)

    //Processing KeyWord Write Records

    val kwWrite = logMaster.filter(logMaster("Source") === "MV_Keyword").filter(logMaster("Summary") === "Write")
    if (kwWrite.count() >1) {
      // Keyword Write Report
      val kwDF0 = kwWrite.select(col("Id"),col("UserName"),col("LogDateTime"))
      val rddKw = kwWrite.select(toJson1(kwWrite("Id"), kwWrite("Details")) as "jsonKey").rdd.map(Row => Row.getString(0))
      val jsonKwWrite = sqlC.read.json(rddKw)
      val kwDF1 = jsonKwWrite.select("Id","Details.entityId","Details.entityType","Details.fields").select(col("Id").as("Id"),col("entityId").as("entityId"),col("entityType").as("entityType"),org.apache.spark.sql.functions.explode(col("fields")).as("non"))
      val kwDF2 = kwDF1.select(col("Id"),col("entityId"),col("entityType"),col("non").getField("fieldName").as("fieldName"),col("non").getField("newValue").as("newValue"),col("non").getField("oldValue").as("oldValue"))
      val kwDF3 = kwDF2.filter(col("entityType") === "KEYWORD")
      kwDF0.registerTempTable("t1")
      kwDF3.registerTempTable("t2")
      val kwDF4 = sqlC.sql("select t1.Id,t1.UserName,t1.LogDateTime,t2.entityId,t2.fieldName,t2.newValue,t2.oldValue from t2 join t1 on t2.Id = t1.Id")
      kwDF4.select("entityId","Id").distinct().orderBy("Id").persist().registerTempTable("t3")
      kwdMaster1.select(col("Id").as("entityId"),col("Name"),col("StateId"),col("AdGroupId")).persist().registerTempTable("t4")
      val kwDF5 = sqlC.sql("select t3.Id,t3.entityId,t4.Name,t4.StateId,t4.AdGroupId from t3 left outer join t4 on (t3.entityId = t4.entityId)")
      kwDF4.registerTempTable("t5")
      kwDF5.registerTempTable("t6")
      val kwDF7 = sqlC.sql("select t6.Id,t6.AdGroupId,t5.UserName,t5.LogDateTime,t6.Name,t5.fieldName,t5.newValue,t5.oldValue,t6.StateId from t6 left outer join t5 on t6.Id = t5.Id")
      val kwRpt = kwDF7.toDF("AUDIT ID","ADGROUP ID","USER ID","LOG DATE TIME","NAME","KEYWORD","NEW VALUE","OLD VALUE","STATE ID")
      kwRpt.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save(dirKwRpt)

      //Keyword Write Summary Report
      val kwSummary1 = kwRpt
      val kwSummary2 = kwSummary1.groupBy(kwSummary1("ADGROUP ID")).count().orderBy(kwSummary1("ADGROUP ID"))
      val kwSummary3 = kwSummary1.join(kwSummary2, "ADGROUP ID").orderBy("ADGROUP ID")
      val dfAdGroup = sqlC.read.option("delimiter", "|").format("com.databricks.spark.csv").option("inferSchema", "true").load(dirAdGroup).select("C0", "C1", "C2").toDF("ADGROUP ID", "ADGROUP NAME", "CAMPAIGN ID")
      val dfAdvertiser = sqlC.read.option("delimiter", "|").format("com.databricks.spark.csv").option("inferSchema", "true").load(dirAdvertiser).select("C0", "C1").toDF("ADV ID", "ADV NAME")
      val dfCampaign = sqlC.read.option("delimiter", "|").format("com.databricks.spark.csv").option("inferSchema", "true").load(dirCampaign).select("C0", "C1", "C2").toDF("CAMPAIGN ID", "ADV ID", "CAMPAIGN NAME")
      val kwSummary4 = dfAdGroup.join(dfCampaign, "CAMPAIGN ID").join(dfAdvertiser, "ADV ID").orderBy("ADGROUP ID")
      val kwSummary5 = kwSummary3.join(kwSummary4, "ADGROUP ID").select("USER ID", "ADGROUP ID", "ADV NAME", "ADGROUP NAME", "CAMPAIGN NAME", "STATE ID", "count", "KEYWORD").toDF("USER ID", "ADGROUP ID", "COMPANY", "ADGROUP NAME", "CAMPAIGN NAME", "STATUS", "TOTAL KW EDITS", "MODIFIED VALUE").distinct()
      kwSummary5.orderBy("ADGROUP ID").repartition(1).write.format("com.databricks.spark.csv").option("delimiter", "|").save(dirSummaryRpt)
    }

    //Processing MV_Keyword Create Records
    val kwCreate = logMaster.filter(logMaster("Source") === "MV_Keyword").filter(logMaster("Summary") === "Create")
    if (kwCreate.count() >1) {
      val kcDF0 = kwCreate.select(col("Id"),col("UserName"),col("LogDateTime"))
      val rddKc = kwCreate.select(toJson1(kwCreate("Id"), kwCreate("Details")) as "jsonKey").rdd.map(Row => Row.getString(0))
      val jsonKwCreate = sqlC.read.json(rddKc)
      val kcDF1 = jsonKwCreate.select("Id","Details.entityId","Details.fields").select(col("Id").as("Id"),col("entityId").as("entityId"),org.apache.spark.sql.functions.explode(col("fields")).as("non"))
      val kcDF2 = kcDF1.select(col("Id"),col("entityId"),col("non").getField("fieldName").as("fieldName"),col("non").getField("newValue").as("newValue"),col("non").getField("oldValue").as("oldValue"))
      val kcDF3 = kcDF2.filter(col("fieldName") === "id").select(col("Id"),col("newValue").as("entityId"))
      val kcDF4 = kcDF2.filter(col("fieldName") === "name").select(col("Id"),col("newValue").as("name"))
      val kcDF5 = kcDF2.filter(col("fieldName") === "createdDateTime").select(col("Id"),col("newValue").as("createdDateTime"))
      val kcDF6 = kcDF2.filter(col("fieldName") === "lastModifiedDateTime").select(col("Id"),col("newValue").as("lastModifiedDateTime"))
      val kcDF7 = kcDF2.filter(col("fieldName") === "adGroupId").select(col("Id"),col("newValue").as("adGroupId"))
      val kcDF8 = kcDF2.filter(col("fieldName") === "stateId").select(col("Id"),col("newValue").as("stateId"))
      val kcDF9 = kcDF3.join(kcDF4,"Id").join(kcDF5,"Id").join(kcDF6,"Id").join(kcDF7,"Id").join(kcDF8,"Id")
      val kcRpt = kcDF0.join(kcDF9,"Id").toDF("AUDIT ID","USER ID","LOG DATE TIME","KEYWORD ID","NAME","CREATED","LAST MODIFIED","ADGROUP ID","STATE ID")
      kcRpt.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save(dirKcRpt)
    }
    sc.stop()
  }
}
