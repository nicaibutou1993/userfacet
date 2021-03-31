package com.jdd.api.util

import java.net.InetAddress

import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.script.ScriptType
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder
import org.elasticsearch.search.aggregations.metrics.max.Max
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient

/**
  * Created by jdd on 2017/3/16.
  */
object EsFactory extends Serializable{

  private val LOCK = new Object()

  var client : TransportClient = null
  def getEsClient (): TransportClient = {
    LOCK.synchronized {
      if(null == client){
        /* val settings: Settings = Settings.builder().put("cluster.name", CommonResource.ES_CLUSTER_NAME).build()
         client = new PreBuiltTransportClient(settings)*/
        client = new PreBuiltXPackTransportClient(Settings.builder()
          .put("cluster.name", CommonResource.ES_CLUSTER_NAME)
          .put("xpack.security.user", s"${CommonResource.ES_USER}:${CommonResource.ES_PASSWORD}")
          .build())

        for(serverName <- CommonResource.ES_NODES.split(",")){
          client .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(serverName), 9300))
        }
      }
    }
    client
  }

  def close (): Unit ={
    try{
      if(null != client){
        client.close()
        client = null
      }
    }catch {
      case e : Exception => {
        /*e.printStackTrace()*/
      }
    }
  }

  def main(args: Array[String]): Unit = {
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
      .setRequest(new SearchRequest().indices("user").types("user_base_info"))
      .get()
      .getResponse()

   val max : Max  = response.getAggregations.get("max_uid")
    println(max.getValue)
  }

}
