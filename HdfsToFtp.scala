package com.multiview.utils



import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by anagarasan on 6/28/16.
  */

object HdfsToFtp {

  def main(args: Array[String]): Unit = {

    val dirNetFactor = "/Users/anagarasan/Documents/DMP/multiview/netfactor/10-10-2016.result"
    val dirFTP = "/Users/anagarasan/Documents/DMP/multiview/netfactor/ftp"

    val conf = new SparkConf().setAppName("FTP-OUT").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //val df = sqlContext.read.format("parquet").load("hdfs//ip/folder/file.gz.parquet")
    //df01.write.text("ftp://<user>:<password>@<host>:<port>/<url-path>")

    /*val df = sqlContext.read.
      format("com.springml.spark.sftp").
      option("host", "sftp.bombora.com").
      option("username", "multiview").
      option("password", "xbRzfDqgvY0x").
      option("fileType", "csv").
      option("inferSchema", "true").
      load("/sftpdirectory/multiview/MultiView_Events_20160811.csv.gz")

    //Write dataframe as CSV file to FTP server

    df.write.
      format("com.springml.spark.sftp").
      option("host", "SFTP_HOST").
      option("username", "SFTP_USER").
      option("password", "****").
      option("fileType", "csv").
      save("/ftp/files/sample.csv")*/


    //Insert data from DataFrame

    // UAT
    /*
    val url = "jdbc:sqlserver://10.84.0.149:1433;database=multi-ui"
    val username = "anagarasan"
    val pwd = "Mv005675!" */

    //PROD

    /*val url = "jdbc:sqlserver://10.85.0.214:1433;database=multi-ui"
    val username = "AppDataLake"
    val pwd = "F8RMGCdcdlq!@#kCapE5JDl"

    DriverManager.registerDriver(new com.microsoft.sqlserver.jdbc.SQLServerDriver)

    val conn = DriverManager.getConnection(url,username,pwd)

    val SQL = conn.prepareStatement(s"select * from dbo.MV_AuditLog where AuditTypeId = 4 AND Source = 'MV_Keyword'  ")

    //AND Summary = 'Write'

    val resultSet = SQL.executeQuery()

    while (resultSet.next()) {
      val id = resultSet.getString("Id")
      val details = resultSet.getString("Details")
      val uName = resultSet.getString("Username")
      val summary = resultSet.getString("Summary")
      println(id+" , "+details+" , "+uName+" , "+summary)
    }

    conn.close()

    val dfNF = sqlContext.read.format("parquet").load(dirNetFactor)
    dfNF.printSchema()
    dfNF.show()*/



    sc.stop()

  }

}