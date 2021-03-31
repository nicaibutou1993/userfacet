package com.jdd.api.util

import java.io.InputStream
import java.util.Properties

/**
  * Created by jdd on 2016/9/14.
  */
object CommonResource extends Serializable{

  private val props : Properties = new Properties()
  private val stream: InputStream = CommonResource.getClass.getClassLoader.getResourceAsStream("config.properties")

  try {
    props.load(stream)
  }catch {
    case e : Exception => e.printStackTrace()
  }

  val FSDEFAULTFS: String = props.getProperty("fs.defaultFS","server-4:8020")
  val HADOOP_USER_NAME: String = props.getProperty("hadoop_user_name","hdfs")
  val HDFSPATH: String = props.getProperty("hdfspath","")
  val SPARK_SQL_SHUFFLE_PARTITIONS: String = props.getProperty("spark.sql.shuffle.partitions","20")
  val BLOCKSIZE: Long = props.getProperty("block_size","134217728").toLong

  val HIVE_DRIVER_NAME: String = props.getProperty("hive_driver_Name","org.apache.hive.jdbc.HiveDriver")
  val HIVE_URL: String = props.getProperty("hive_url","jdbc:hive2://server-4:10000/default")
  val HIVE_USER: String = props.getProperty("hive_user","spark")
  val HIVE_PASSWORD: String = props.getProperty("hive_password","spark")
  val MYSQL_CONNECTION_URL: String = props.getProperty("MYSQL_CONNECTION_URL","jdbc:mysql://bigdata:3306/datacenter_userface")
  val DRIVER: String = props.getProperty("DRIVER","com.mysql.jdbc.Driver")
  val ES_NODES: String = props.getProperty("es.nodes","server-1,server-2,server-3")
  val ES_PORT: String = props.getProperty("es.port","9200")
  val ES_PATH: String = props.getProperty("es.path","/trade_orders/orders")
  val ES_USER: String = props.getProperty("es.user","elastic")
  val ES_PASSWORD: String = props.getProperty("es.password","zUX8k7nsr8Cr7waC")

  val MYSQL_CONNECTION_URL_DATACENTER_USERFACE: String = props.getProperty("mysql_datacenter_userface_url","jdbc:mysql://bigdata:3306/datacenter_userface")
  val ES_CLUSTER_NAME: String = props.getProperty("es.cluster.name","facet-search")


  val JDD_USER_URL: String = props.getProperty("JDD_USER_URL","jdbc:mysql://116.90.86.16:3306/jdd_user?characterEncoding=utf8&amp;autoReconnect=true&amp;failOverReadOnly=false")
  val JDD_USER_USER: String = props.getProperty("JDD_USER_USER","jdd_user")
  val JDD_USER_PASSWORD: String = props.getProperty("JDD_USER_PASSWORD","dgjdd_userpassword")

  val JDD_USER_TABLENAME: String = props.getProperty("JDD_USER_TABLENAME","user_info")

}
