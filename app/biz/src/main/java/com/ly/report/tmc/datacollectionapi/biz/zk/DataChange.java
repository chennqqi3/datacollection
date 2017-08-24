package com.ly.report.tmc.datacollectionapi.biz.zk;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.mortbay.util.TypeUtil.newLong;

/**
 * Created by hyz46086 on 2017/5/5.
 */
public class DataChange {

    public static void main(String args[]){

        SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Long time=newLong(1482301758000L);
        String d = format.format(time);
        Date date= null;
        try {
            date = format.parse(d);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        System.out.println("Format To String(Date):"+d);
        System.out.println("Format To Date:"+date);

    }

}
