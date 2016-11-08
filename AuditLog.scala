package com.multiview.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by anagarasan on 6/24/16.
  */

object AuditLog {
  def main(args: Array[String]) {

    val inputDate = args(0)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal1 = Calendar.getInstance()
    cal1.setTime(dateFormat.parse(inputDate))
    cal1.add(Calendar.DAY_OF_YEAR,-1)
    val curr_date = dateFormat.format(cal1.getTime)
    //val curr_date = "2016-10-16"
    val dirLogMaster = s"/multiview/auditlog/source/dt=$curr_date"
    //val dirLogMaster = s"/Users/anagarasan/Documents/DMP/multiview/AuditLog/source/dt=$curr_date"
    val dirTarget = "/multiview/auditlog"
    //val dirTarget = "/Users/anagarasan/Documents/DMP/multiview/AuditLog"

    val conf = new SparkConf().setAppName("AuditLog").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlC = new HiveContext(sc)

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
    logMaster.write.save(s"$dirTarget/logmaster/dt=$curr_date")

    val customConcat1 = (Id: Int, Details: String) => {s""" { "Id" : $Id, "Details":$Details } """}
    val toJson1 = udf(customConcat1)
    val customConcat2 = (eid: Int, n: String, o: String) => {s""" { "eid" : $eid, "DetailsN":$n, "DetailsO":$o } """}
    val toJson2 = udf(customConcat2)

    //Processing ADGROUP Records

    val adGroup = df0.filter(df0("Source") === "MV_AdGroup")
    if (adGroup.count() >1) {
    val ds1 = adGroup.select(toJson1(adGroup("Id"), adGroup("Details")) as "jsonKey").rdd.map(Row => Row.getString(0))
    val jsonAdGroup = sqlC.read.json(ds1)
    jsonAdGroup.select("Id","Details.entityId","Details.entityType","Details.parentId","Details.fields.fieldName","Details.fields.newValue","Details.fields.oldValue").write.save(s"$dirTarget/logadgroup/dt=$curr_date")

    val ag01 = jsonAdGroup.select("Id","Details.fields").select(col("Id").as("eid"),org.apache.spark.sql.functions.explode(col("fields")).as("non"))
    val ag02 = ag01.select(col("eid"),col("non").getField("fieldName").as("fieldName"),col("non").getField("newValue").as("newValue"),col("non").getField("oldValue").as("oldValue"))

    // Processing ADGROUP - dayParting

    val dp01 = ag02.filter(col("fieldName")==="dayPartings")
    if (dp01.count()>1) {
    val dp02 = dp01.select(toJson2(ag02("eid"), ag02("newValue"), ag02("oldValue"))).rdd.map(Row => Row.getString(0))
    val dpN = sqlC.read.json(dp02).select("eid","DetailsN").select(col("eid").as("eid"),org.apache.spark.sql.functions.explode(col("DetailsN")).as("n"))
      dpN.select(dpN.col("eid").as("eid")
        ,dpN.col("n").getField("bidAdjustment").as("NewBidAdjustment")
        ,dpN.col("n").getField("dayOfWeek").as("NewDayOfWeek")
        ,dpN.col("n").getField("dayPartingId").as("NewDayPartingId")
        ,dpN.col("n").getField("endHour").as("NewEndHour")
        ,dpN.col("n").getField("startHour").as("NewStartHour")).write.save(s"$dirTarget/adgroup/dayparting/new/dt=$curr_date")

    val dpOSchema = sqlC.read.json(dp02).select("DetailsO").schema.toString()
    if (dpOSchema.contains("ArrayType")) {
    val dpO = sqlC.read.json(dp02).select("eid","DetailsO").select(col("eid").as("eid"),org.apache.spark.sql.functions.explode(col("DetailsO")).as("o"))
    dpO.select(dpO.col("eid").as("eid")
      ,dpO.col("o").getField("bidAdjustment").as("OldBidAdjustment")
      ,dpO.col("o").getField("dayOfWeek").as("OldDayOfWeek")
      ,dpO.col("o").getField("dayPartingId").as("OldDayPartingId")
      ,dpO.col("o").getField("endHour").as("OldEndHour")
      ,dpO.col("o").getField("startHour").as("OldStartHour")).write.save(s"$dirTarget/adgroup/dayparting/old/dt=$curr_date")}
    else {sqlC.read.json(dp02).select(col("eid").as("eid"),col("DetailsO").as("OldValue")).write.save(s"$dirTarget/adgroup/dayparting/old/dt=$curr_date")}}

    //Processing ADGROUP - adGroupRecencies

    val agRecencies01 = ag02.filter(col("fieldName")==="adGroupRecencies")
    if (agRecencies01.count()>1)  {
    val agRecencies02 = agRecencies01.select(toJson2(ag02("eid"), ag02("newValue"), ag02("oldValue"))).rdd.map(Row => Row.getString(0))
    val agrN = sqlC.read.json(agRecencies02).select("eid","DetailsN").select(col("eid").as("eid"),org.apache.spark.sql.functions.explode(col("DetailsN")).as("n"))
    agrN.select(col("eid").as("eid")
      ,col("n").getField("adjustment").as("NewAdjustment")
      ,col("n").getField("frequencyId").as("NewFrequencyId")
      ,col("n").getField("recencyId").as("NewRecencyId")
    ,col("n").getField("recencyWindow").as("NewRecencyWindow")).write.save(s"$dirTarget/adgroup/agRecencies/new/dt=$curr_date")

    val agRSchema = sqlC.read.json(agRecencies02).select("DetailsO").schema.toString()
    if (agRSchema.contains("ArrayType")){
      val agrO = sqlC.read.json(agRecencies02).select("eid","DetailsO").select(col("eid").as("eid"),org.apache.spark.sql.functions.explode(col("DetailsO")).as("o"))
      agrO.select(col("eid").as("eid")
      ,col("o").getField("adjustment").as("OldAdjustment")
      ,col("o").getField("frequencyId").as("OldFrequencyId")
      ,col("o").getField("recencyId").as("OldRecencyId")
      ,col("o").getField("recencyWindow").as("OldRecencyWindow")).write.save(s"$dirTarget/adgroup/agRecencies/old/dt=$curr_date")}
    else {sqlC.read.json(agRecencies02).select(col("eid").as("eid"),col("DetailsO").as("OldValue")).write.save(s"$dirTarget/adgroup/agRecencies/old/dt=$curr_date")}}

    //Processing ADGROUP - adGroupToTDDeviceTypes

    val agToTDDeviceTypes01 = ag02.filter(col("fieldName")==="adGroupToTDDeviceTypes")
    if (agToTDDeviceTypes01.count()>1) {
    val agToTDDeviceTypes02 = agToTDDeviceTypes01.select(toJson2(ag02("eid"), ag02("newValue"), ag02("oldValue"))).rdd.map(Row => Row.getString(0))
    val agTTDDTN = sqlC.read.json(agToTDDeviceTypes02).select("eid","DetailsN").select(col("eid").as("eid"),org.apache.spark.sql.functions.explode(col("DetailsN")).as("n"))

      agTTDDTN.select(col("eid").as("eid")
        ,col("n").getField("bidAdjustment").as("NewBidAdjustment")
        ,col("n").getField("tradeDeskDeviceTypeId").as("NewTDDeviceTypeId")).write.save(s"$dirTarget/adgroup/tddevicetypes/new/dt=$curr_date")

    val agTTDTOSchema = sqlC.read.json(agToTDDeviceTypes02).select("DetailsO").schema.toString()
      if (agTTDTOSchema.contains("ArrayType")) {
       val agTTDDTO = sqlC.read.json(agToTDDeviceTypes02).select("eid","DetailsO").select(col("eid").as("eid"),org.apache.spark.sql.functions.explode(col("DetailsO")).as("o"))
           agTTDDTO.select(col("eid").as("eid")
          ,col("o").getField("bidAdjustment").as("OldBidAdjustment")
          ,col("o").getField("tradeDeskDeviceTypeId").as("OldTDDeviceTypeId")).write.save(s"$dirTarget/adgroup/tddevicetypes/old/dt=$curr_date")}
      else {sqlC.read.json(agToTDDeviceTypes02).select(col("eid").as("eid"),col("DetailsO").as("OldValue")).write.save(s"$dirTarget/adgroup/tddevicetypes/old/dt=$curr_date")}
    }

    //Processing ADGROUP - AudienceId

    val agAudienceId01 = ag02.filter(col("fieldName")==="audienceId")
    if (agAudienceId01.count()>1){
    val agAudienceId02 = agAudienceId01.select(toJson2(ag02("eid"), ag02("newValue"), ag02("oldValue"))).rdd.map(Row => Row.getString(0))
    sqlC.read.json(agAudienceId02).select(col("eid").as("eid"),col("DetailsO").as("OldAudienceId"),col("DetailsN").as("NewAudienceId")).write.save(s"$dirTarget/adgroup/audienceid/dt=$curr_date")}

    //Processing ADGROUP - Geo Fences

    val agGeoFences01 = ag02.filter(col("fieldName")==="geoFences")
    if (agGeoFences01.count()>1){
    val agGeoFences02 = agGeoFences01.select(toJson2(ag02("eid"), ag02("newValue"), ag02("oldValue"))).rdd.map(Row => Row.getString(0))
    val agGFN = sqlC.read.json(agGeoFences02).select("eid","DetailsN").select(col("eid").as("eid"),org.apache.spark.sql.functions.explode(col("DetailsN")).as("n"))
    agGFN.select(col("eid").as("eid")
      ,col("n").getField("coordinates").getField("x").as("NewCoordinatesX")
      ,col("n").getField("coordinates").getField("y").as("NewCoordinatesY")
      ,col("n").getField("geoFenceTypeId").as("NewGeoFenceTypeId")).write.save(s"$dirTarget/adgroup/geofences/new/dt=$curr_date")

    val agGFSchema = sqlC.read.json(agGeoFences02).select("DetailsN").schema.toString()
    if (agGFSchema.contains("ArrayType")){
      val agGFO = sqlC.read.json(agGeoFences02).select("eid","DetailsO").select(col("eid").as("eid"),org.apache.spark.sql.functions.explode(col("DetailsO")).as("o"))
      agGFO.select(col("eid").as("eid")
        ,col("o").getField("coordinates").getField("x").as("OldCoordinatesX")
        ,col("o").getField("coordinates").getField("y").as("OldCoordinatesY")
      ,col("o").getField("geoFenceTypeId").as("OldGeoFenceTypeId")).write.save(s"$dirTarget/adgroup/geofences/old/dt=$curr_date")}
    else {sqlC.read.json(agGeoFences02).select(col("eid").as("eid"),col("DetailsO").as("OldValue")).write.save(s"$dirTarget/adgroup/geofences/old/dt=$curr_date")}}

    // Processing ADGROUP - Postal Codes

    val agPostalCodes01 = ag02.filter(col("fieldName")==="postalCodes")
    if (agPostalCodes01.count()>1){
    val agPostalCodes02 = agPostalCodes01.select(toJson2(ag02("eid"), ag02("newValue"), ag02("oldValue"))).rdd.map(Row => Row.getString(0))
    sqlC.read.json(agPostalCodes02).select("eid","DetailsN").select(col("eid").as("eid"),org.apache.spark.sql.functions.explode(col("DetailsN")).as("NewPostCodes")).write.save(s"$dirTarget/adgroup/postal_codes/new/dt=$curr_date")
    val agPCSchema = sqlC.read.json(agPostalCodes02).select("DetailsO").schema.toString()
    if (agPCSchema.contains("ArrayType"))
      {sqlC.read.json(agPostalCodes02).select("eid","DetailsO").select(col("eid").as("eid"),org.apache.spark.sql.functions.explode(col("DetailsO")).as("OldPostCodes")).write.save(s"$dirTarget/adgroup/postal_codes/old/dt=$curr_date")}
    else
      {sqlC.read.json(agPostalCodes02).select(col("eid").as("eid"),col("DetailsO").as("OldPostCodes")).write.save(s"$dirTarget/adgroup/postal_codes/old/dt=$curr_date")}}}
    // Processing AUDIENCE Records

    val audience = df0.filter(df0("Source") === "Audience")
    if (audience.count()>1) {
    val ds2 = audience.select(toJson1(col("Id"), col("Details")) as "jsonKey").rdd.map(Row => Row.getString(0))
    val jsonAudience = sqlC.read.json(ds2)
    jsonAudience.write.save(s"$dirTarget/audience/dt=$curr_date")}

    // Processing SITE Records

    val mvSite = df0.filter(df0("Source") === "MV_Site")
    if (mvSite.count()>1) {
    val ds3 = mvSite.select(toJson1(col("Id"), col("Details")) as "jsonKey").rdd.map(Row => Row.getString(0))
    val jsonSite = sqlC.read.json(ds3)
    jsonSite.select("Id","Details.entityId","Details.entityType","Details.fields.fieldName","Details.fields.newValue","Details.fields.oldValue").write.save(s"$dirTarget/site/dt=$curr_date")

    val site01 = jsonSite.select("Id","Details.fields").select(col("Id").as("eid"),org.apache.spark.sql.functions.explode(col("fields")).as("non"))
    val site02 = site01.select(col("eid"),col("non").getField("fieldName").as("fieldName"),col("non").getField("newValue").as("newValue"),col("non").getField("oldValue").as("oldValue"))
    val siteUrls = site02.filter(col("fieldName")==="urls").select(toJson2(col("eid"), col("newValue"), col("oldValue"))).rdd.map(Row => Row.getString(0))
    sqlC.read.json(siteUrls).select(col("eid").as("Id"),col("DetailsN").as("NewUrl"),col("DetailsO").as("OldUrl")).write.save(s"$dirTarget/siteurls/dt=$curr_date")}

    // Processing CREATIVE Records

    val mvCreative = df0.filter(df0("Source") === "MV_Creative")
    if (mvCreative.count()>1) {
    val ds4 = mvCreative.select(toJson1(col("Id"), col("Details")) as "jsonKey").rdd.map(Row => Row.getString(0))
    sqlC.read.json(ds4).select("Id","Details.entityId","Details.entityType","Details.parentId","Details.fields.fieldName","Details.fields.newValue","Details.fields.oldValue").write.save(s"$dirTarget/creative/dt=$curr_date")}

    // Processing ADVERTISER Records

    val mvAdvertiser = df0.filter(col("Source") === "MV_Advertiser")
    if (mvAdvertiser.count()>1) {
    val ds5 = mvAdvertiser.select(toJson1(col("Id"),col("Details")).as("jsonKey")).rdd.map(Row => Row.getString(0))
    val jsonAdvertiser = sqlC.read.json(ds5)
    jsonAdvertiser.select("Id","Details.entityId","Details.entityType","Details.fields.fieldName","Details.fields.newValue","Details.fields.oldValue").write.save(s"$dirTarget/advertiser/dt=$curr_date")

    val adv01 = jsonAdvertiser.select("Id","Details.fields").select(col("Id").as("eid"),org.apache.spark.sql.functions.explode(col("fields")).as("non"))
    val adv02 = adv01.select(col("eid"),col("non").getField("fieldName").as("fieldName"),col("non").getField("newValue").as("newValue"),col("non").getField("oldValue").as("oldValue"))
    adv02.write.save(s"$dirTarget/advertiser_fields/dt=$curr_date") }

    // Processing ACTION Records

    val mvAction = df0.filter(col("Source") === "MV_Action")
    if (mvAction.count()>1) {
    val ds6 = mvAction.select(toJson1(col("Id"),col("Details")).as("jsonkey")).rdd.map(Row => Row.getString(0))
    val jsonAction = sqlC.read.json(ds6)
    jsonAction.select("Id","Details.entityId","Details.entityType","Details.fields.fieldName","Details.fields.newValue","Details.fields.oldValue").write.save(s"$dirTarget/action/dt=$curr_date")

    val act01 = jsonAction.select("Id","Details.fields").select(col("Id").as("eid"),org.apache.spark.sql.functions.explode(col("fields")).as("non"))
    val act02 = act01.select(col("eid"),col("non").getField("fieldName").as("fieldName"),col("non").getField("newValue").as("newValue"),col("non").getField("oldValue").as("oldValue"))
    act02.write.save(s"$dirTarget/action_fields/dt=$curr_date") }

    // Processing CAMPAIGN Records

    val mvCampaign = df0.filter(col("Source") === "MV_Campaign")
    if (mvCampaign.count()>1) {
    val ds7 = mvCampaign.select(toJson1(col("Id"),col("Details")).as("jsonkey")).rdd.map(Row => Row.getString(0))
    val jsonCampaign = sqlC.read.json(ds7)
    jsonCampaign.select("Id","Details.entityId","Details.entityType","Details.parentId","Details.fields.fieldName","Details.fields.newValue","Details.fields.oldValue").write.save(s"$dirTarget/campaign/dt=$curr_date")

    val cmp01 = jsonCampaign.select("Id","Details.fields").select(col("Id").as("eid"),org.apache.spark.sql.functions.explode(col("fields")).as("non"))
    val cmp02 = cmp01.select(col("eid"),col("non").getField("fieldName").as("fieldName"),col("non").getField("newValue").as("newValue"),col("non").getField("oldValue").as("oldValue"))
    cmp02.write.save(s"$dirTarget/campaign_fields/dt=$curr_date")}

    // Processing GLOBAL DOMAIN Records

    val mvGlbDomain = df0.filter(col("Source") === "MV_GlobalDomain")
    if(mvGlbDomain.count() > 1) {
      val ds8 = mvGlbDomain.select(toJson1(col("Id"),col("Details")).as("jsonkey")).rdd.map(Row => Row.getString(0))
      val jsonGDomain = sqlC.read.json(ds8)
      jsonGDomain.select("Id","Details.entityId","Details.parentId","Details.fields.fieldName","Details.fields.newValue","Details.fields.oldValue").write.save(s"$dirTarget/global_domain/dt=$curr_date")

      val gDmn01 = jsonGDomain.select("Id","Details.fields").select(col("Id").as("eid"),org.apache.spark.sql.functions.explode(col("fields")).as("non"))
      val gDmn02 = gDmn01.select(col("eid"),col("non").getField("fieldName").as("fieldName"),col("non").getField("newValue").as("newValue"),col("non").getField("oldValue").as("oldValue"))
      gDmn02.write.save(s"$dirTarget/global_domain_fields/dt=$curr_date") }

    // Processing GLOBAL DOMAIN LIST Records

    val mvGlbDomainList = df0.filter(col("Source") === "MV_GlobalDomainList")
    if (mvGlbDomainList.count() > 1) {
    val ds9 = mvGlbDomainList.select(toJson1(col("Id"),col("Details")).as("jsonkey")).rdd.map(Row => Row.getString(0))
    val jsonGDomainList = sqlC.read.json(ds9)
    jsonGDomainList.select("Id","Details.entityId","Details.entityType","Details.parentId","Details.fields.fieldName","Details.fields.newValue","Details.fields.oldValue").write.save(s"$dirTarget/global_domain_list/dt=$curr_date")

    val gDmnList01 = jsonGDomainList.select("Id","Details.fields").select(col("Id").as("eid"),org.apache.spark.sql.functions.explode(col("fields")).as("non"))
    val gDmnList02 = gDmnList01.select(col("eid"),col("non").getField("fieldName").as("fieldName"),col("non").getField("newValue").as("newValue"),col("non").getField("oldValue").as("oldValue"))
    gDmnList02.write.save(s"$dirTarget/global_domain_list_fields/dt=$curr_date") }

    //Processing ADVERTISER DOMAIN Records

    val mvAdvDmn = df0.filter(col("Source") === "MV_AdvertiserDomain")
    if (mvAdvDmn.count() >1) {
    val ds10 = mvAdvDmn.select(toJson1(col("Id"),col("Details")).as("jsonkey")).rdd.map(Row => Row.getString(0))
    val jsonAdvDmn = sqlC.read.json(ds10)
    jsonAdvDmn.select("Id","Details.entityId","Details.parentId","Details.fields.fieldName","Details.fields.newValue","Details.fields.oldValue").write.save(s"$dirTarget/advertiser_domain/dt=$curr_date")

    val aDmn01 = jsonAdvDmn.select("Id","Details.fields").select(col("Id").as("eid"),org.apache.spark.sql.functions.explode(col("fields")).as("non"))
    val aDmn02 = aDmn01.select(col("eid"),col("non").getField("fieldName").as("fieldName"),col("non").getField("newValue").as("newValue"),col("non").getField("oldValue").as("oldValue"))
    aDmn02.write.save(s"$dirTarget/advertiser_domain_fields/dt=$curr_date") }

    // Processing ADVERTISER DOMAIN LIST Records

    val mvAdvDmnList = df0.filter(col("Source") === "MV_AdvertiserDomainList")
    if (mvAdvDmnList.count() >1) {
    val ds11 = mvAdvDmnList.select(toJson1(col("Id"),col("Details")).as("jsonkey")).rdd.map(Row => Row.getString(0))
    val jsonAdvDmnList = sqlC.read.json(ds11)
    jsonAdvDmnList.select("Id","Details.entityId","Details.entityType","Details.parentId","Details.fields.fieldName","Details.fields.newValue","Details.fields.oldValue").write.save(s"$dirTarget/advertiser_domain_list/dt=$curr_date")

    val aDmnList01 = jsonAdvDmnList.select("Id","Details.fields").select(col("Id").as("eid"),org.apache.spark.sql.functions.explode(col("fields")).as("non"))
    val aDmnList02 = aDmnList01.select(col("eid"),col("non").getField("fieldName").as("fieldName"),col("non").getField("newValue").as("newValue"),col("non").getField("oldValue").as("oldValue"))
    aDmnList02.write.save(s"$dirTarget/advertiser_domain_list_fields/dt=$curr_date") }

    sc.stop()
  }
}
