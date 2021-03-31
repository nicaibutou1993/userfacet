package com.jdd.api.main

import com.jdd.api.util.CommonResource
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.Metadata._
import org.elasticsearch.spark.rdd.{EsSpark, Metadata}

import scala.collection.Map
/**
  * Created by jdd on 2017/5/3.
  */
object UserBaseInfoImportEs {

  private val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  private val _format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd")

  val ES_INDEX = "user"
  val ES_TYPE = "user_base_info"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    var sc : SparkContext = null
    try{

      val conf: SparkConf = new SparkConf().setMaster("local[4]")
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("es.net.http.auth.user", CommonResource.ES_USER)
      conf.set("es.net.http.auth.pass", CommonResource.ES_PASSWORD)
      conf.set("es.nodes", CommonResource.ES_NODES)
      conf.set("es.port", CommonResource.ES_PORT)

      val sparkSession: SparkSession = SparkSession
        .builder()
        .appName("UserBaseInfoImportEs")
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
      sc = sparkSession.sparkContext

      import sparkSession.implicits._

      //file:///home/user/spark/README.md
      val linesRDD: RDD[String] = sc.textFile("file:\\C:\\Users\\jdd\\Downloads\\user_info.txt")

      val iniUserDF: DataFrame = linesRDD.map(row => {
        var tuple: (Long, String, String, String, String,String, String, String, String ) = (0L,"","",null,"","","","","")
        try {
          val lineSplit: Array[String] = row.split("#",-1)
          val uid: Long = lineSplit(0).trim.toLong
          val userName: String = lineSplit(1).trim
          val registerdate: String = lineSplit(2).trim

          val time: Long = format.parse(registerdate).getTime
          val registertime: String = format.format(time)

          val registerday: String = _format.format(time)

          val channel: String = lineSplit(3).trim
          val phone_os: String = lineSplit(4).trim
          var registerplateform: String = lineSplit(5).trim.toLowerCase
          if ("iphone".equals(registerplateform)) registerplateform = "ios"
          val app_version: String = lineSplit(6).trim
          val phone_name: String = lineSplit(7).trim
          tuple = (uid, userName, registerday, registertime, registerplateform, app_version, channel, phone_os, phone_name)
        }catch {
          case e : Exception => println(row + "\n")
        }

        tuple
      }).filter(_._1 != 0).toDF("uid", "username", "register_day", "register_time", "register_plateform", "register_app_version", "register_channel", "register_phone_os", "register_phone_name")

      val map: RDD[(Map[Metadata, Long], Map[String, Any])] = iniUserDF.rdd.map(row => {

        val map: Map[String, Any] = Map("uid" -> row.getAs[Long]("uid"),"username" -> row.getAs[String]("username"), "register_day" -> row.getAs[String]("register_day"),
          "register_time" -> row.getAs[String]("register_time"), "register_plateform" -> row.getAs[String]("register_plateform"),
          "register_app_version" -> row.getAs[String]("register_app_version"), "register_channel" -> row.getAs[String]("register_channel"),
          "register_phone_os" -> row.getAs[String]("register_phone_os"), "register_phone_name" -> row.getAs[String]("register_phone_name")
)
        (Map(ID -> row.getAs[Long]("uid")), map)
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
