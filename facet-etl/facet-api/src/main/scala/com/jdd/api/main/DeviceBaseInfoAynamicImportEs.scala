package com.jdd.api.main

import java.text.SimpleDateFormat

import com.jdd.api.util.CommonResource
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.Metadata._
import org.elasticsearch.spark.rdd.{EsSpark, Metadata}

import scala.collection.Map

/**
  * Created by jdd on 2017/5/3.
  */
object DeviceBaseInfoAynamicImportEs {

  private val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  val ES_INDEX = "user"
  val ES_TYPE = "device_base_info"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    var sc : SparkContext = null
    try{

      val conf: SparkConf = new SparkConf()
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("es.net.http.auth.user", CommonResource.ES_USER)
      conf.set("es.net.http.auth.pass", CommonResource.ES_PASSWORD)
      conf.set("es.nodes", CommonResource.ES_NODES)
      conf.set("es.port", CommonResource.ES_PORT)
      conf.set("es.write.operation", "upsert")

      val sparkSession: SparkSession = SparkSession
        .builder()
        .appName("DeviceBaseInfoAynamicImportEs")
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
      sc = sparkSession.sparkContext

      import sparkSession.sql

      val startDay: String = new SimpleDateFormat("yyyy-MM-dd").format(System.currentTimeMillis() - 60 * 60 * 24 * 1000L * 7)

      val map: RDD[(Map[Metadata, String], Map[String, Any])] = sql(s"select uid, registerday, deviceid, registertime, registerplateform, channel, version from facet.facet_base_devices where registerday >= '$startDay'").rdd.map(row => {
        val map: Map[String, Any] = Map("uid" -> row.getAs[Long]("uid"), "deviceid" -> row.getAs[String]("deviceid"), "register_day" -> row.getAs[String]("registerday"),
          "register_time" -> format.format(row.getAs[Long]("registertime")), "register_plateform" -> row.getAs[String]("registerplateform"),
          "register_app_version" -> row.getAs[String]("version"), "register_channel" -> row.getAs[String]("channel")
        )
        (Map(ID -> row.getAs[String]("deviceid")), map)
      })

      EsSpark.saveToEsWithMeta(map,s"/$ES_INDEX/$ES_TYPE")

    }catch {
      case e : Exception => {e.printStackTrace()}
    }finally {
      if(null != sc){
        sc.stop
      }
    }
  }
}
