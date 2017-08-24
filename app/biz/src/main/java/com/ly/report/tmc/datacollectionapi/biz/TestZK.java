package com.ly.report.tmc.datacollectionapi.biz;

import com.ly.report.tmc.datacollectionapi.biz.util.StringHelper;
import com.ly.report.tmc.datacollectionapi.biz.zk.ZookeeperExecutor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by hyz46086 on 2017/4/18.
 */
public class TestZK
{

    public static String dateToStamp(String s) throws ParseException {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(s);
        long ts = date.getTime();
        res = String.valueOf(ts);
        return res;
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


    public static String TimeStamp2Date(String timestampString, String formats){
        Long timestamp = Long.parseLong(timestampString)*1000;
        String date = new java.text.SimpleDateFormat(formats).format(new java.util.Date(timestamp));
        return date;
    }

    public static void main(String args[]) throws ParseException {


        System.out.println(new Date().getTime());


        System.out.println(StringHelper.deleteLine("2017-01-02"));
        System.out.println(new Date().getTime());
//        System.out.println(StringHelper.deleteLine(TimeStamp2Date("-28800000","yyyy-MM-dd HH:mm:ss")));
//        System.out.println(StringHelper.deleteLine(dateToStamp("2017-05-05 00:00:00")));
//        System.out.println(StringHelper.deleteLine(stampToDate("-28800000")));
//        System.out.println(StringHelper.deleteLine(stampToDate("1487582589000")));


//        FlightTask flightTask = new FlightTask();
//        TaskDO taskDO =  new TaskDO();
//        taskDO.setState(0);
//        taskDO.setDescription("");
//        taskDO.setCreatetime(new Date());
//        flightTask.setTaskDO(taskDO);
//        JSON.toJSONString( flightTask);
//        System.out.print(JSON.toJSONString( flightTask));
//        System.out.print("test");

//        ZookeeperExecutor zke= ZookeeperExecutor.getInstance();
//        zke.watchChildrenPath("/TCTMCstatisticsNode",new PreliminaryNodeChildrenCallback(), PathChildrenCacheEvent.Type.CHILD_UPDATED);

//        zke.createPath("/Tmctest","2000".getBytes());
//        zke.setPath("/Tmctest","8000");
//        System.out.println(new String(zke.getPath("/Tmctest")) );


//       zke.createPath("/Tmctest1","120".getBytes());
//       zke.createPath("/TCTMCstatisticsNode/Tmctest2","2000".getBytes());
//       zke.createPath("/TCTMCstatisticsNode/Tmctest3","2001".getBytes());
//       zke.createPath("/TCTMCstatisticsNode/Tmctest2","2002".getBytes());
//        System.out.println(new String(zke.getPath("/Tmctest2")) );
//        System.out.println(new String(zke.getPath("/Tmctest2")) );
//        System.out.println(new String(zke.getPath("/Tmctest2")) );
//        System.out.println(new String(zke.getPath("/TCTMCstatisticsNode/Tmctest3")) );
//        zke.setPath("/TCTMCstatisticsNode/Tmctest3","1000");
//        System.out.println(new String(zke.getPath("/TCTMCstatisticsNode/Tmctest3")) );
//        zke.setPath("/TCTMCstatisticsNode/Tmctest2","000");
//        System.out.println(new String(zke.getPath("/TCTMCstatisticsNode/topic_4")) );
//        System.out.println(new String(zke.getPath("/TCTMCstatisticsNode/topic_5")) );
//        zke.deletePath("/Tmctest");
//        zke.deletePath("/TCTMCstatisticsNode");
//        zke.deletePath("/TCTMCstatisticsNode/refund_topic");
//        zke.deletePath("/TCTMCstatisticsNode/topic");
//        zke.deletePath("/TCTMCstatisticsNode/topic_1");
//        zke.deletePath("/TCTMCstatisticsNode/topic_2");
//        zke.deletePath("/TCTMCstatisticsNode/topic_3");
//        zke.deletePath("/TCTMCstatisticsNode/topic_4");
//        zke.deletePath("/TCTMCstatisticsNode/createNum");
//        zke.deletePath("/TCTMCstatisticsNode/refundNum");
//        zke.deletePath("/TCTMCstatisticsNode/refundNum");

//        System.out.println(new String(zke.getPath("/TCTMCstatisticsNode")) );
//        System.out.println(new String(zke.getPath("/TCTMCstatisticsNode/refund_topic")) );
//        System.out.println(new String(zke.getPath("/TCTMCstatisticsNode/topic")) );
//        System.out.println(new String(zke.getPath("/TCTMCstatisticsNode/createNum")) );
//        System.out.println(new String(zke.getPath("/TCTMCstatisticsNode/refundNum")) );


//        zke.setPath("/TCTMCstatisticsNode/Tmctest2","7000");

//        System.out.println(new String(zke.getPath("/TCTMCstatisticsNode/Tmctest")) );


//        zke.setPath("/TCTMCstatisticsNode/Tmctest","5");
//        System.out.println( zke.getPath("/TCTMCstatisticsNode/Tmctest").toString());

    }
}
