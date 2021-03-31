package com.jdd.api.bean

import com.alibaba.fastjson.JSONObject

/**
  * Created by jdd on 2017/3/17.
  */
case class FacetLogsBean (
                           uid : Long,
                           deviceid : String,
                           plateform : String,
                           eventid: Int,
                           eventcontent :JSONObject,
                           begintime : Long,
                           uploadtime : Long,
                           ip : String,
                           version : String,
                           pmenu : String,
                           menu: String,
                           net: String,
                           channel: String,
                           client: String,
                           os: String,
                           subplateform: String,
                           p_day: String,
                           hour : Int,
                           minute : Int
                    ) extends Serializable{

}
