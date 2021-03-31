package com.jdd.api.service

import java.io.Serializable
import java.net.URL
import java.util

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable


/**
  * Created by jdd on 2017/3/17.
  */
object FacetLogsImportEsService extends Serializable{


  val ES_DB_NAME : String = "facet"
  val ES_TABLE_NAME : String = "facet_logs"


  /**
    * 动态为eventContent 设值
    * @param mappingArray
    */
  def setMappingValue(mappingArray: mutable.HashMap[String,(String, String, String, String,String)], _plateform : String,eventcontent : String): JSONObject ={

    val sourceObject: JSONObject = JSON.parseObject(eventcontent)
    try {
      if(null != mappingArray ){

        val parseObject: JSONObject =JSON.parseObject( sourceObject.getString("source"))
        if(null != parseObject && parseObject.size() >0){
          val iterator: util.Iterator[String] = parseObject.keySet().iterator()
          //val mappingAtr: mutable.HashMap[String,(String, String, String, String,String)] = mappingArray._1
          while (iterator.hasNext){
            val k: String = iterator.next().trim.toLowerCase
            val attr: (String, String, String, String,String) = mappingArray.getOrElse(k,null)
            //attribute,(target_attribute,datatype,ps,es_ps)
            if(null != attr){
              val plateform: String = attr._5
              if(null != plateform && ("all".equalsIgnoreCase(plateform) || _plateform.equalsIgnoreCase(plateform))){
                val v: String = parseObject.getOrDefault(k,"").toString
                val target_attribute: String = attr._1
                val datatype: String = attr._2

                datatype match {
                  case "url" =>{
                    val psSplit: Array[String] = attr._3.split(",")
                    val es_psSplit: Array[String] = attr._4.split(",")
                    val psMap: Map[String, String] = psSplit.zip(es_psSplit).toMap

                    val url: URL = new java.net.URL(v)
                    var path: String = url.getPath

                    if(null != path )path = path.replaceAll("//","/")

                    val domain: String = url.getHost
                    sourceObject.put("domain",if(null == domain) "" else domain)
                    sourceObject.put("path",if(null == path) "" else path)
                    val urlParts: Array[String] = v.split("\\?")
                    if (urlParts.length > 1) {
                      val query: String = urlParts(1)
                      for ( param <- query.split("&")) {
                        val pair: Array[String] = param.split("=")
                        if(pair.length == 2){
                          val pCol : String = psMap.getOrElse(pair(0),null)
                          if(null != pCol && !pCol.trim.equals("")){
                            sourceObject.put(pCol,if(null == pair(1)) "" else pair(1))
                          }
                        }
                      }
                    }
                  }
                  case "string" =>{
                    sourceObject.put(target_attribute,if(null == v) "" else v)
                  }
                }
              }
            }
          }
        }
      }

    }catch {
      case e : Exception => {
        e.printStackTrace()}
    }
    sourceObject
  }


  def contentToJson(plateform : String, eventcontent : String): String ={
    val contentJson: JSONObject = new JSONObject()
    var _eventcontent = eventcontent
    try{

      if("ios".equals(plateform)){
        if(null != eventcontent && !eventcontent.trim.equals("")){
          val obj: JSONObject = new JSONObject()
          val split: Array[String] = eventcontent.replaceAll("\\{","").replaceAll("\\}","").replaceAll("\\;",",").replaceAll("\\t","").split(",")
          for(atr <- split){
            val kv: Array[String] = atr.split("=")
            if(kv.size == 2){
              obj.put(kv(0).trim,kv(1).trim)
            }
          }
          if(obj.size() > 0){
            _eventcontent = obj.toJSONString
          }
        }
      }
    }catch {
      case e : Exception =>{e.printStackTrace()}
    }
    contentJson.put("source",_eventcontent)
    contentJson.toJSONString
  }

}
