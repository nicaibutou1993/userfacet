package com.jdd.api.main

import java.sql.Timestamp

import com.alibaba.fastjson.JSONObject
import com.jdd.api.service.FacetLogsImportEsService
import com.jdd.api.util.{CommonResource, JDBCUtil}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.Metadata
import org.elasticsearch.spark.rdd.Metadata._

import scala.collection.{Map, mutable}

/**
  * Created by jdd on 2017/3/17.
  */
object FacetLogsImportEs {

  private val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    var sc : SparkContext = null
    try {

      val conf: SparkConf = new SparkConf()/*.setAppName("FacetLogsImportEs")*//*.setMaster("local[2]")*/
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("es.net.http.auth.user", CommonResource.ES_USER)
      conf.set("es.net.http.auth.pass", CommonResource.ES_PASSWORD)
      conf.set("es.nodes", CommonResource.ES_NODES)
      conf.set("es.port", CommonResource.ES_PORT)

      val sparkSession: SparkSession = SparkSession
        .builder()
        .appName("FacetLogsImportEs")
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
      sc = sparkSession.sparkContext

      import sparkSession.sql

      val startDay: String = args(0)

      val endDay: String = args(1)

      val mapping: mutable.HashMap[Long, mutable.HashMap[String, (String, String, String, String, String)]] = JDBCUtil.queryMapping()

      val broadcast: Broadcast[mutable.HashMap[Long, mutable.HashMap[String, (String, String, String, String, String)]]] = sc.broadcast(mapping)

      val facetLogsDF: DataFrame = sql(s"select uid,deviceid,plateform,eventid,eventcontent,begintime,uploadtime,ip,version,pmenu,menu,net,channel,client,os,subplateform,p_day from facet.facet_logs where p_day >= '$startDay' and p_day <= '$endDay'").persist(StorageLevel.MEMORY_AND_DISK_SER)

      val map: RDD[(Map[Metadata, String], Map[String, Any])] = facetLogsDF.rdd.map(row => {
        val mappingMap: mutable.HashMap[Long, mutable.HashMap[String, (String, String, String, String, String)]] = broadcast.value
        val uid: Long = row.getAs[Long]("uid")
        val deviceid: String = row.getAs[String]("deviceid")
        val eventid: Int = row.getAs[Int]("eventid")
        val begintime: Long = row.getAs[Long]("begintime")
        val uploadtime: Long = row.getAs[Long]("uploadtime")

        val timestamp: Timestamp = new Timestamp(begintime)
        val hours: Int = timestamp.getHours
        val minutes: Int = timestamp.getMinutes

        val plateform : String = row.getAs[String]("plateform")
        val eventcontent: String = row.getAs[String]("eventcontent")

        val value: JSONObject = FacetLogsImportEsService.setMappingValue(mappingMap.getOrElse(eventid,null),plateform,FacetLogsImportEsService.contentToJson(plateform,eventcontent))

        var tuple: (Map[Metadata, String], Map[String, Any]) = null
        if (uid == 0 && (null == deviceid || "".equals(deviceid.trim))) {

        } else {
          val hex: String = DigestUtils.md5Hex(uid + deviceid + eventid + begintime)

          val map: Map[String, Any] = Map("uid" -> uid, "deviceid" -> deviceid, "plateform" -> plateform,
            "eventid" -> eventid, "eventcontent" -> value, "begintime" -> begintime,
            "uploadtime" -> uploadtime, "ip" -> row.getAs[String]("ip"), "version" -> row.getAs[String]("version"),
            "pmenu" -> row.getAs[String]("pmenu"), "menu" -> row.getAs[String]("menu"), "net" -> row.getAs[String]("net"),
            "channel" -> row.getAs[String]("channel"), "client" -> row.getAs[String]("client"), "os" -> row.getAs[String]("os"),
            "plateform" -> row.getAs[String]("plateform"), "p_day" -> row.getAs[String]("p_day"), "hour" -> hours,"minute"->minutes,
            "eventtime"->format.format(begintime))
          tuple = (Map(ID -> hex), map)
        }
        tuple
      }).filter(_ != null)

      map.saveToEsWithMeta("/facet/facet_logs")
    }catch {
      case e : Exception => e.printStackTrace()
    }finally {
      if(null != sc){
        sc.stop
      }
    }
  }
}
