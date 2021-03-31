package com.jdd.statistics.util; /**
 * Created by jdd on 2017/4/13.
 */


import org.apache.commons.lang3.time.FastDateFormat;
import scala.Tuple3;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 * 类说明:提取身份证相关信息
 * </p>
 */
public class IdcardInfoExtractor implements Serializable{

    private static FastDateFormat format = FastDateFormat.getInstance("yyyyMMdd");
    private static FastDateFormat _format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    private static Map<String, String> cityCodeMap = new HashMap<String, String>() {
        {
            this.put("11", "北京");
            this.put("12", "天津");
            this.put("13", "河北");
            this.put("14", "山西");
            this.put("15", "内蒙古");
            this.put("21", "辽宁");
            this.put("22", "吉林");
            this.put("23", "黑龙江");
            this.put("31", "上海");
            this.put("32", "江苏");
            this.put("33", "浙江");
            this.put("34", "安徽");
            this.put("35", "福建");
            this.put("36", "江西");
            this.put("37", "山东");
            this.put("41", "河南");
            this.put("42", "湖北");
            this.put("43", "湖南");
            this.put("44", "广东");
            this.put("45", "广西");
            this.put("46", "海南");
            this.put("50", "重庆");
            this.put("51", "四川");
            this.put("52", "贵州");
            this.put("53", "云南");
            this.put("54", "西藏");
            this.put("61", "陕西");
            this.put("62", "甘肃");
            this.put("63", "青海");
            this.put("64", "宁夏");
            this.put("65", "新疆");
            this.put("71", "台湾");
            this.put("81", "香港");
            this.put("82", "澳门");
            this.put("91", "国外");
        }
    };


    /**
     * 通过构造方法初始化各个成员属性
     */
    public static String getBaseInfo(String idcard) {
        String province = "";
        Integer gender = -1;
        String birthday = "";

        try {
            if( null == idcard ) return gender + "_" + province + "_" + birthday;
            idcard = idcard.trim();
            if (IdcardValidator.isValidatedAllIdcard(idcard)) {
                if (idcard.length() == 15) {
                    idcard = IdcardValidator.convertIdcarBy15bit(idcard);
                }
                // 获取省份
                String provinceId = idcard.substring(0, 2);
                Set<String> key = cityCodeMap.keySet();
                for (String id : key) {
                    if (id.equals(provinceId)) {
                        province = cityCodeMap.get(id);
                        break;
                    }
                }

                // 获取性别
                String id17 = idcard.substring(16, 17);
                if (Integer.parseInt(id17) % 2 != 0) {
                    gender = 1;
                } else {
                    gender = 2;
                }

                // 获取出生日期
                birthday = _format.format(format.parse(idcard.substring(6, 14)));
                /*Date birthdate = new SimpleDateFormat("yyyyMMdd")
                        .parse(birthday);
                this.birthday = birthdate;*/
                /*GregorianCalendar currentDay = new GregorianCalendar();
                currentDay.setTime(birthdate);
                this.year = currentDay.get(Calendar.YEAR);
                this.month = currentDay.get(Calendar.MONTH) + 1;
                this.day = currentDay.get(Calendar.DAY_OF_MONTH);*/
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return gender + "_" + province + "_" + birthday;
    }

    public static void main(String[] args) {
        String idcard = "321023199309173032";
        IdcardInfoExtractor idcardInfoExtractor = new IdcardInfoExtractor();
        idcardInfoExtractor.getBaseInfo(idcard);

        /*com.jdd.statistics.util.IdcardInfoExtractor ie = new com.jdd.statistics.util.IdcardInfoExtractor(idcard);
        System.out.println(ie.getGender());
        System.out.println(ie.toString());*/
    }
}
