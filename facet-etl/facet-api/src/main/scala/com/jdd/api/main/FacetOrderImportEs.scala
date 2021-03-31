package com.jdd.api.main

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.jdd.api.util.CommonResource
import org.apache.commons.lang3.time.{DateUtils, FastDateFormat}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.{EsSpark, Metadata}
import org.elasticsearch.spark.rdd.Metadata._

import scala.collection.Map
/**
  * Created by jdd on 2017/3/16.
  */
object FacetOrderImportEs {

  val ES_INDEX = "trade"
  val ES_TYPE = "trade_orders"

  private val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    var sc : SparkContext = null
    try {
      val startTime: Long = System.currentTimeMillis() - DateUtils.MILLIS_PER_DAY * 2

      val day: String = new SimpleDateFormat("yyyy-MM-dd").format(startTime )

      val conf: SparkConf = new SparkConf().setAppName("FacetOrderImportEs")/*.setMaster("local[2]")*/

      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("es.net.http.auth.user", CommonResource.ES_USER)
      conf.set("es.net.http.auth.pass", CommonResource.ES_PASSWORD)
      conf.set("es.nodes", CommonResource.ES_NODES)
      conf.set("es.port", CommonResource.ES_PORT)

      val sparkSession: SparkSession = SparkSession
        .builder()
        .appName("FacetOrderImportEs")
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
      sc = sparkSession.sparkContext

      import sparkSession.implicits._
      import sparkSession.sql

      val esQueryDsl : String =
        s"""
           |{
           |  "query": {
           |    "bool": {
           |      "must": [
           |        {
           |          "range": {
           |            "p_day": {
           |              "gte": "$day"
           |            }
           |          }
           |        }
           |      ]
           |    }
           |  }
           |}
          """.stripMargin

      val esUuidRdd: RDD[(String, Map[String, AnyRef])] = EsSpark.esRDD(sc,s"/$ES_INDEX/$ES_TYPE",esQueryDsl,Map("es.read.field.include" -> "_id"))

      val existUuidDF: DataFrame = esUuidRdd.map (row => row._1).toDF("uuid")

      existUuidDF.show(10)

      val facetOrderDF: DataFrame = sql(s"select uuid,uid,schemeid,issueno,lotteryid,playtypeid,superplaytypeid,betnumber,hteamid,hteam,vteamid,vteam,tournamentid,tournamentname,matchresult,iswin,datetime,platformcode,multiple,bunch,betsp,winsp,money,p_day from trade.trade_orders where p_day >= '$day'").persist(StorageLevel.MEMORY_AND_DISK_SER)

      facetOrderDF.show(10)

      val needAddUuidDF: DataFrame = facetOrderDF.select("uuid").except(existUuidDF)

      needAddUuidDF.show(10)

      val needAddOrderDF: DataFrame = facetOrderDF.join(needAddUuidDF,facetOrderDF.col("uuid").equalTo(needAddUuidDF.col("uuid")),"leftsemi")

      val map: RDD[(Map[Metadata, String], Map[String, Any])] = needAddOrderDF.rdd.map(row => {
        val map: Map[String, Any] = Map("uid" -> row.getAs[Long]("uid"), "schemeid" -> row.getAs[Long]("schemeid"), "issueno" -> row.getAs[String]("issueno"),
          "lotteryid" -> row.getAs[Int]("lotteryid"), "playtypeid" -> row.getAs[Int]("playtypeid"), "superplaytypeid" -> row.getAs[Int]("superplaytypeid"),
          "betnumber" -> row.getAs[String]("betnumber"), "hteamid" -> row.getAs[Int]("hteamid"), "hteam" -> row.getAs[String]("hteam"),
          "vteamid" -> row.getAs[Int]("vteamid"), "vteam" -> row.getAs[String]("vteam"), "tournamentid" -> row.getAs[Long]("tournamentid"),
          "tournamentname" -> row.getAs[String]("tournamentname"), "matchresult" -> row.getAs[String]("matchresult"), "iswin" -> row.getAs[Int]("iswin"),
          "datetime" -> format.format(row.getAs[Timestamp]("datetime")), "platformcode" -> row.getAs[String]("platformcode"), "multiple" -> row.getAs("multiple"),
          "bunch" -> row.getAs[String]("bunch"), "betsp" -> row.getAs[String]("betsp"), "winsp" -> row.getAs[String]("winsp"), "money" -> row.getAs[Double]("money"),
          "p_day" -> row.getAs[String]("p_day"))
        (Map(ID -> row.getAs[String]("uuid")), map)
      })
      EsSpark.saveToEsWithMeta(map,s"/$ES_INDEX/$ES_TYPE")
    }catch {
      case e : Exception => e.printStackTrace()
    }finally {
      if(null != sc){
        sc.stop
      }
    }
  }
}
