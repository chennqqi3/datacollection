package com.ly.report.tmc.datacollectionapi.web.controller.util;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by hyz46086 on 2017/5/2.
 */
public class DateUtil {

    /**
     * 将Date类型转换为字符串
     *
     * @param date
     *            日期类型
     * @return 日期字符串
     */
    public static String format(Date date) {
        return format(date, "yyyy-MM-dd HH:mm:ss");
    }

    /**
     * 将Date类型转换为字符串(毫秒)
     *
     * @param date
     *            日期类型
     * @return 日期字符串
     */
    public static String formatSSS(Date date) {
        return format(date, "yyyy-MM-dd HH:mm:ss.SSS");
    }

    /**
     * 将Date类型转换为字符串
     *
     * @param date
     *            日期类型
     * @param pattern
     *            字符串格式
     * @return 日期字符串
     */
    public static String format(Date date, String pattern) {
        if (date == null) {
            return "null";
        }
        if (pattern == null || pattern.equals("") || pattern.equals("null")) {
            pattern = "yyyy-MM-dd HH:mm:ss";
        }
        return new SimpleDateFormat(pattern).format(date);
    }

    /**
     * 将String Date类型转换为 另一种格式字符串
     *
     *            日期类型字符串
     * @param pattern
     *            字符串格式
     * @return 日期字符串
     */
    public static String formatString(String strDate, String pattern) {
        if (strDate == null) {
            return null;
        }
        if (pattern == null || pattern.equals("") || pattern.equals("null")) {
            pattern = "yyyy-MM-dd-HH-mm-ss";
        }
        Date date = null;
        try {
            date = new SimpleDateFormat("yyyy年MM月dd日HH时mm分ss秒").parse(strDate);
        } catch (ParseException pe) {
        }

        return new SimpleDateFormat(pattern).format(date);
    }

    /**
     * 将String Date类型转换为 另一种格式字符串
     *
     * @param pattern
     *            字符串格式
     * @return 日期字符串
     */
    public static String formatString(String strDate, String oldPattern, String pattern) {
        if (strDate == null) {
            return null;
        }
        if (pattern == null || pattern.equals("") || pattern.equals("null")) {
            pattern = "yyyy-MM-dd HH-mm-ss";
        }
        Date date = null;
        try {
            date = new SimpleDateFormat(oldPattern).parse(strDate);
        } catch (ParseException pe) {
        }

        return new SimpleDateFormat(pattern).format(date);
    }

    /**
     * 将字符串转换为Date类型
     *
     * @param date
     *            字符串类型
     * @return 日期类型
     */
    public static Date format(String date) {
        return format(date, null);
    }

    /**
     * 将字符串转换为Date类型
     *
     * @param date
     *            字符串类型
     * @param pattern
     *            格式
     * @return 日期类型
     */
    public static Date format(String date, String pattern) {
        if (pattern == null || pattern.equals("") || pattern.equals("null")) {
            pattern = "yyyy-MM-dd HH:mm:ss";
        }
        if (date == null || date.equals("") || date.equals("null")) {
            return new Date();
        }
        Date d = null;
        try {
            d = new SimpleDateFormat(pattern).parse(date);
        } catch (ParseException pe) {
        }
        return d;
    }

    /**
     * 将字符串转换为Timestamp类型
     *
     * @param dateStr
     *            日期字符串
     * @param pattern
     *            格式
     * @return Timestamp类型
     */
    public static Timestamp toTimestamp(String dateStr, String pattern) {
        Date date = null;
        if (pattern == null || pattern.equals("") || pattern.equals("null")) {
            pattern = "yyyy-MM-dd HH:mm:ss";
        }
        if (dateStr == null || dateStr.equals("") || dateStr.equals("null")) {
            date = new Date();
        } else {
            date = format(dateStr);
        }
        Timestamp ts = new Timestamp(date.getTime());
        return ts;
    }

    /**
     * 获取当前时间
     *
     * @param formatStr
     *            格式字符串 如"yyyy-MM-dd HH:mm:ss"
     *
     */
    public static String getNowTime(String formatStr) {
        SimpleDateFormat sdf = new SimpleDateFormat(formatStr);
        return sdf.format(new Date()).toString();
    }

    /**
     * 获取当前时间 以"yyyy-MM-dd HH:mm:ss"格式返回当前时间
     *
     */
    public static String getNowTime() {
        return getNowTime("yyyy-MM-dd HH:mm:ss");
    }

    /**
     * 获取当前系统时间戳
     *
     * @return
     */
    public static Timestamp getTimestamp() {
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        return ts;
    }

    public static int getYear(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        return c.get(Calendar.YEAR);
    }

    public static String getBeforeDate() {
        Calendar  c = Calendar.getInstance();
        c.add(Calendar.DAY_OF_MONTH, -1);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String mDateTime=formatter.format(c.getTime());
        return mDateTime;
    }



    public static String getNowDate() {
        Calendar  c = Calendar.getInstance();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String mDateTime=formatter.format(c.getTime());
        return mDateTime;
    }

    public static String getYesterdayTime() {
        Calendar  c = Calendar.getInstance();
        c.add(Calendar.DAY_OF_MONTH, -1);
//        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String mDateTime=formatter.format(c.getTime());
        return mDateTime;
    }
    public static String getLastWeekTime() {
        Calendar  c = Calendar.getInstance();
        c.add(Calendar.DAY_OF_MONTH, -7);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String mDateTime=formatter.format(c.getTime());
        return mDateTime;
    }


    public static String getNineTime(){

        long time = System.currentTimeMillis();
        String nowTimeStamp = String.valueOf(time / 1000);
        String nineTime = nowTimeStamp.substring(nowTimeStamp.length()-9,nowTimeStamp.length());
        return nineTime;
    }


    public static Date changeYMD(String ymddate){
        SimpleDateFormat   formatter   = new SimpleDateFormat( "yyyy-MM-dd");
        Date date =null;
        try {
            date =   formatter.parse(ymddate);

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }


    //日期相加
    public static Date getAddCountMonth(Date date,int n){
        try{
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            c.add(Calendar.MONTH, n);
            return c.getTime();
        }catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
