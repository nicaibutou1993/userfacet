package com.jdd.api.main

import com.jdd.api.util.CommonResource
import com.jdd.statistics.util.IdcardInfoExtractor
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.Metadata._
import org.elasticsearch.spark.rdd.{EsSpark, Metadata}

import scala.collection.Map

/**
  * Created by jdd on 2017/5/3.
  */
object UserIdCardAynamicImportEs {

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
        .appName("UserIdCardAynamicImportEs")
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
      sc = sparkSession.sparkContext

      import sparkSession.implicits._

      //
      val userIncDF : DataFrame = sparkSession.read.format("jdbc").options(
        Map("url" -> CommonResource.JDD_USER_URL,
          "dbtable" -> s"(select n_user_id as uid, s_idcard_number as idcard from user_base_info where n_user_id > 10157904 and s_idcard_number is not null ) as user_base_info",
          "driver" ->CommonResource.DRIVER,
          "user" -> CommonResource.JDD_USER_USER,
          "user" -> CommonResource.JDD_USER_USER,
          "password" -> CommonResource.JDD_USER_PASSWORD,
          "numPartitions" -> "2")).load()
        .map(row => {
          val uid: Long = row.getAs[Long]("uid")
          var idcard: String = row.getAs[String]("idcard")
          val userBaseInfo: Array[String] = IdcardInfoExtractor.getBaseInfo(idcard).split("_",-1)
          val gender: Int = userBaseInfo(0).toInt
          if(-1 == gender) idcard = ""
          val birth_province: String = userBaseInfo(1)
          val birthday: String = userBaseInfo(2)
          (uid,idcard,gender,birth_province,birthday)
        }).toDF("uid","idcard", "gender","birth_province","birthday").persist(StorageLevel.MEMORY_AND_DISK_SER)

      val map: RDD[(Map[Metadata, Long], Map[String, Any])] = userIncDF.rdd.map(row => {
        val birthday: String = row.getAs[String]("birthday")
        val gender: Int = row.getAs[Int]("gender")
        val idcard: String = row.getAs[String]("idcard")
        var map: Map[String, Any] = null
          if(!"".equals(birthday) && !"".equals(idcard)) {
            map = Map("uid" -> row.getAs[Long]("uid"),"idcard" -> row.getAs[String]("idcard"), "gender" -> row.getAs[Int]("gender"),
              "birth_province" -> row.getAs[String]("birth_province"), "birthday" -> row.getAs[String]("birthday")
            )
          }
        (Map(ID -> row.getAs[Long]("uid")), map)
      }).filter(_._2 != null)
      EsSpark.saveToEsWithMeta(map,s"/$ES_INDEX/$ES_TYPE")
      userIncDF.unpersist()
    }catch {
      case e : Exception => e.printStackTrace()
    }finally {
      if(null != sc){
        sc.stop
      }
    }
  }
}

