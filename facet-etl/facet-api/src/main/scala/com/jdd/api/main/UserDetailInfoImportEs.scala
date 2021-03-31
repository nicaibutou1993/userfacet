package com.jdd.api.main

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.jdd.api.util.CommonResource
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.Metadata._
import org.elasticsearch.spark.rdd.{EsSpark, Metadata}

import scala.collection.{Map, mutable}

/**
  * Created by jdd on 2017/5/4.
  */
object UserDetailInfoImportEs {

  private val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

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

      val sparkSession: SparkSession = SparkSession
        .builder()
        .appName("UserDetailInfoImportEs")
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
      sc = sparkSession.sparkContext

      import sparkSession.sql

      val startDay : String = args(0)
      val endDay : String = args(1)

      val facetLogs: DataFrame = sql(s"select uid,deviceid,channel,version,plateform,begintime from facet.facet_logs where uid > 0 and deviceid is not null and deviceid != '' and p_day >='${startDay}' and p_day <= '${endDay}'").persist(StorageLevel.MEMORY_AND_DISK_SER_2)

      facetLogs.createOrReplaceTempView("facetLogs")

      val trackDF: DataFrame = sql("select uid,deviceid,channel,version,plateform,begintime from (select uid,deviceid,channel,version,plateform,begintime, ROW_NUMBER() OVER(PARTITION BY uid,deviceid,channel,version,plateform order by begintime asc) as rn from facetLogs) origin where origin.rn = 1")
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      trackDF.show(100)

      val map: RDD[(Map[Metadata, Long], Map[String, Any])] = trackDF.rdd.map(row => {
        val uid: Long = row.getAs[Long]("uid")
        val deviceid: String = row.getAs[String]("deviceid")
        val channel: String = row.getAs[String]("channel")
        val version: String = row.getAs[String]("version")
        val plateform: String = row.getAs[String]("plateform")

        val firstUseTime: String = format.format(row.getAs[Long]("begintime"))
        (uid, (deviceid, channel, version, plateform, firstUseTime))
      }).partitionBy(new UserPartition)
        .mapPartitions(par => {
          val dataMap: mutable.HashMap[Long, JSONArray] = new mutable.HashMap[Long, JSONArray]
          par.foreach(row => {
            val uid: Long = row._1
            val deviceid: String = row._2._1
            val channel: String = row._2._2
            val version: String = row._2._3
            val plateform: String = row._2._4
            val firstUseTime: String = row._2._5

            val array: JSONArray = dataMap.getOrElse(uid, new JSONArray())
            val nObject: JSONObject = new JSONObject()
            nObject.put("deviceid", deviceid)
            nObject.put("channel", channel)
            nObject.put("app_version", version)
            nObject.put("plateform", plateform)
            nObject.put("firstUseTime", firstUseTime)
            array.add(nObject)
            dataMap.put(uid, array)
          })
          dataMap.iterator
        }).map(row => {
        val uid: Long = row._1
        val array: JSONArray = row._2
        val map: Map[String, Any] = Map("uid" -> uid, "base_track" -> array)
        (Map(ID -> uid), map)
      })

      EsSpark.saveToEsWithMeta(map,s"/$ES_INDEX/$ES_TYPE")

      facetLogs.unpersist()
      trackDF.unpersist()
    }catch {
      case e : Exception => e.printStackTrace()
    }finally {
      if(null != sc){
        sc.stop
      }
    }

  }
}

class UserPartition() extends Partitioner with Serializable{
  override def numPartitions: Int = 20
  override def getPartition(key: Any): Int = {
    var code = key.toString.hashCode % 20
    if (code < 0) {
      code = code + 20
    }
    code
  }
}
