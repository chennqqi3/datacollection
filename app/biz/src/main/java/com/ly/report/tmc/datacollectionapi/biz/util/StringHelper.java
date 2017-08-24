package com.ly.report.tmc.datacollectionapi.biz.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class StringHelper {
    /**
     * 判断字符串是否为空或是空字符串
     *@param value
     *@return boolean
     *@author hcw
     */
    public static boolean isNullOrEmpty(String value){    	
        if(value==null||value.isEmpty()){
            return true;
        }
        else{
            return false;
        }
    }

    /**
     *
     * @param dateTime
     */
    public static String deleteLine(String dateTime){
        String dealString="";
        if(!(isNullOrEmpty(dateTime))){
            String[] lineArray = dateTime.split("-");
            for(int i=0;i<lineArray.length;i++){
                dealString+=lineArray[i];
            }
        }
        return dealString;
    }


    /*
    * 将时间戳转换为时间
    */
    public static String stampToDate(String s){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long lt = new Long(s);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }
}