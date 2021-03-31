package com.jdd.api.main

import java.sql.Timestamp

import com.jdd.api.util.{CommonResource, EsFactory}
import org.apache.commons.lang3.time.{DateUtils, FastDateFormat}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.script.ScriptType
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder
import org.elasticsearch.search.aggregations.metrics.max.Max
import org.elasticsearch.spark.rdd.Metadata._
import org.elasticsearch.spark.rdd.{EsSpark, Metadata}

import scala.collection.Map

/**
  * Created by jdd on 2017/5/3.
  */
object UserBaseInfoAynamicImportEs {

  private val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  private val _format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd")

  val ES_INDEX = "user"
  val ES_TYPE = "user_base_info"

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
        .appName("UserBaseInfoAynamicImportEs")
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
      sc = sparkSession.sparkContext

      import sparkSession.implicits._

      val maxUid: Long = getMaxUserId

      if(maxUid > 0L){

        val registerTimeDelay: Timestamp = new Timestamp(System.currentTimeMillis() - 5 * DateUtils.MILLIS_PER_MINUTE)
        val userIncDF : DataFrame = sparkSession.read.format("jdbc").options(
          Map("url" -> CommonResource.JDD_USER_URL,
            "dbtable" -> s"(select n_user_id as uid,s_login_name as username,d_register_time as registerdate,s_platform_code as registerplateform,s_app_version as app_version,s_entrance_code as channel, s_platform_version as phone_os,s_phone_name as phone_name  from ${CommonResource.JDD_USER_TABLENAME} where n_user_id > ${maxUid} and d_register_time < '$registerTimeDelay') as user_base_info",
            "driver" ->CommonResource.DRIVER,
            "user" -> CommonResource.JDD_USER_USER,
            "password" -> CommonResource.JDD_USER_PASSWORD)).load()
          .map(row => {
            val uid: Long = row.getAs[Long]("uid")
            val timestamp: Timestamp = row.getAs[Timestamp]("registerdate")
            val registertime: String = format.format(timestamp)
            val registerday: String = _format.format(timestamp)
            val channel: String = if(null == row.getAs[String]("channel")) "" else row.getAs[String]("channel").trim
            val username: String = if(null == row.getAs[String]("username")) "" else row.getAs[String]("username").trim
            val phone_os: String = if(null == row.getAs[String]("phone_os")) "" else row.getAs[String]("phone_os").trim
            val phone_name: String = if(null == row.getAs[String]("phone_name")) "" else row.getAs[String]("phone_name").trim
            var registerplateform: String = if(null == row.getAs[String]("registerplateform")) "" else row.getAs[String]("registerplateform").trim.toLowerCase
            if(registerplateform.equals("iphone")){
              registerplateform = "ios"
            }
            val app_version: String = if(null == row.getAs[String]("app_version")) "" else row.getAs[String]("app_version")
            (uid, username,registerday, registertime,registerplateform,app_version,channel,phone_os,phone_name)
          }).toDF("uid","username", "register_day","register_time","register_plateform","register_app_version","register_channel","register_phone_os","register_phone_name").persist(StorageLevel.MEMORY_AND_DISK_SER)

        val map: RDD[(Map[Metadata, Long], Map[String, Any])] = userIncDF.rdd.map(row => {

          val map: Map[String, Any] = Map("uid" -> row.getAs[Long]("uid"),"username" -> row.getAs[String]("username"), "register_day" -> row.getAs[String]("register_day"),
            "register_time" -> row.getAs[String]("register_time"), "register_plateform" -> row.getAs[String]("register_plateform"),
            "register_app_version" -> row.getAs[String]("register_app_version"), "register_channel" -> row.getAs[String]("register_channel"),
            "register_phone_os" -> row.getAs[String]("register_phone_os"), "register_phone_name" -> row.getAs[String]("register_phone_name")
          )
          (Map(ID -> row.getAs[Long]("uid")), map)
        })
        EsSpark.saveToEsWithMeta(map,s"/$ES_INDEX/$ES_TYPE")

      }
    }catch {
      case e : Exception => {}/*e.printStackTrace()*/
    }finally {
      if(null != sc){
        sc.stop
      }
    }

  }

  def getMaxUserId(): Long ={

    var max_uid : Long = -1L
    try{
      val esClient: TransportClient = EsFactory.getEsClient

      val esQueryDsl : String =
        s"""{
            |  "size": 0,
            |  "aggs": {
            |    "max_uid": {
            |      "max": {
            |        "field": "uid"
            |      }
            |    }
            |  }
            |}
          """.stripMargin

      val response: SearchResponse = new SearchTemplateRequestBuilder(esClient)
        .setScript(esQueryDsl)
        .setScriptType(ScriptType.INLINE)
        .setRequest(new SearchRequest().indices(ES_INDEX).types(ES_TYPE))
        .get()
        .getResponse()

      val max : Max  = response.getAggregations.get("max_uid")
      max_uid = max.getValue.toLong

      println(max_uid)
    }catch {
      case e : Exception =>{e.printStackTrace()}
    }finally {
      EsFactory.close()
    }
    max_uid
  }

}

