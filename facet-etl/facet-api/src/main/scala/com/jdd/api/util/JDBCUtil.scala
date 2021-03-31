package com.jdd.api.util

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
  * Created by jdd on 2016/11/1.
  */
object JDBCUtil {

  var conn: Connection = null

  def ini: Unit ={
    if(null == conn || conn.isClosed){
      Class.forName(CommonResource.DRIVER)
      //加载数据库引擎，返回给定字符串名的类
      try {
        conn = DriverManager.getConnection(CommonResource.MYSQL_CONNECTION_URL_DATACENTER_USERFACE)
      }catch {
        case e : Exception => try {
          conn = DriverManager.getConnection(CommonResource.MYSQL_CONNECTION_URL_DATACENTER_USERFACE)
        }catch {
          case e : Exception => e.printStackTrace()
        }
      }
    }
  }

  /**
    * @return
    */
  def queryMapping(): mutable.HashMap[Long, mutable.HashMap[String,(String, String, String, String,String)]]={

    val mappingMap: mutable.HashMap[Long, mutable.HashMap[String,(String, String, String, String,String)]] = new HashMap[Long,mutable.HashMap[String,( String, String, String, String,String)]]
    var stmt: Statement = null
    try {
      ini
      stmt = conn.createStatement()

      val sql = s"select a.action_id,a.template_id from facet_action_attributes_template_mapper a left join facet_action_attributes_template b on a.template_id = b.id"
      val result: ResultSet = stmt.executeQuery(sql)

      val action_template_plateform: ArrayBuffer[(Long, Long)] = new ArrayBuffer[(Long,Long)]()
      while (result.next()){
        val actionId: Long = result.getLong(1)
        val templateId: Long = result.getLong(2)
        /*val plateform: String = result.getString(3)*/
        action_template_plateform += ((actionId,templateId))
      }

      val templateDetailSql = s"select template_id,attribute,target_attribute,datatype,ps,es_ps,plateform from facet_action_attributes_template_detail where status = 1"
      val templateDetailResult: ResultSet = stmt.executeQuery(templateDetailSql)

      val templateDetailMap: mutable.HashMap[Long, mutable.HashMap[String,(String, String, String, String,String)]] = new mutable.HashMap[Long,mutable.HashMap[String,(String, String, String, String,String)]]()
      while (templateDetailResult.next()){
        val templateId: Long = templateDetailResult.getLong(1)
        val attribute: String = templateDetailResult.getString(2)
        val target_attribute: String = templateDetailResult.getString(3)
        val datatype: String = templateDetailResult.getString(4)
        val ps: String = templateDetailResult.getString(5)
        val es_ps: String = templateDetailResult.getString(6)
        val plateform: String = templateDetailResult.getString(7)

        val templateDetailArray: HashMap[String,(String, String, String, String,String)] = templateDetailMap.getOrElse(templateId,new HashMap[String,(String, String, String, String,String)]())
        templateDetailArray.put(attribute,(target_attribute,datatype,ps,es_ps,plateform))
        templateDetailMap.put(templateId,templateDetailArray)
      }
      action_template_plateform.foreach(row => {
        val templateDetailArray: mutable.HashMap[String,(String, String, String, String,String)] = templateDetailMap.getOrElse(row._2,new mutable.HashMap[String,(String, String, String, String,String)])
        mappingMap.put(row._1,templateDetailArray)
      }
      )
    }catch {
      case e : Exception => e.printStackTrace()
    }finally {
      if ( null != stmt ){
        stmt.close()
      }
    }
    println(mappingMap.size + mappingMap.mkString)
    mappingMap
  }

  def close: Unit ={
    try {
      if(null != conn){
        conn.close()
      }
    }catch {
      case e : Exception => e.printStackTrace()
    }
  }



}
