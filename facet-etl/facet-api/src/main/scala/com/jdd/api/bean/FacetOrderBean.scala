package com.jdd.api.bean

import java.sql.Timestamp

/**
  * Created by jdd on 2017/3/16.
  */

case class  FacetOrderBean(uid : Long	,
                           schemeid : Long	,
                           issueno : String	,
                           lotteryid : Int	,
                           playtypeid : Int	,
                           superplaytypeid : Int	,
                           betnumber : String	,
                           hteamid : Int	,
                           hteam : String	,
                           vteamid : Int	,
                           vteam : String	,
                           tournamentid : Long	,
                           tournamentname : String	,
                           matchresult : String	,
                           iswin : Int	,
                           datetime : Timestamp	,
                           platformcode : String	,
                           multiple :  Int	,
                           bunch : String	,
                           betsp : String,
                           winsp : String	,
                           p_day : String) extends Serializable{
}
