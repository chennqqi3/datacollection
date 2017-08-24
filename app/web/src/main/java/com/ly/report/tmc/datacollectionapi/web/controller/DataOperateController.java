/**
 * LY.com Inc.
 * Copyright (c) 2004-2017 All Rights Reserved.
 */
package com.ly.report.tmc.datacollectionapi.web.controller;

import com.alibaba.fastjson.JSONObject;
import com.ly.report.tmc.datacollectionapi.biz.analyze.DataAnalyze;
import com.ly.report.tmc.datacollectionapi.biz.zk.ZookeeperExecutor;
import com.ly.report.tmc.datacollectionapi.dal.common.DalConstant;
import com.ly.report.tmc.datacollectionapi.dal.hive.db.HiveDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author hyz46086
 * @version $Id: DataOperateController, v 0.1 2017/5/22 15:00 hyz46086 Exp $
 */
@Controller
public class DataOperateController {

    @Resource
    private HiveDAO hiveDAO;

    /** logger */
    private Logger logger = LoggerFactory.getLogger(DataOperateController.class);

    @ResponseBody
    @RequestMapping(value = "/getData" ,method = RequestMethod.POST)
    public List<Map<String, Object>> getData(@RequestParam(value = "tableName", required = true)  final String tableName){
        String sql = "select * from "+tableName ;
        List<Map<String, Object>> objectList =null;
        try {
           objectList = hiveDAO.executeQuerySQL(sql);
        } catch (SQLException e) {
            logger.error("<DataOperateController><getData><getData><getData><getDataError>" + "【获取所有数据】topic数据分析，入参="
                    + ",异常..........."+org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
            e.printStackTrace();
        }
        return objectList;
    }


    @RequestMapping(value = "/deleteTable" ,method = RequestMethod.GET)
    public void deleteTable(){
        String sql = "DROP TABLE IF EXISTS "+ DalConstant.DATABASE+".DEPARTMENT_ORDER_INFO_partition";
        try {
            hiveDAO.createTables(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @RequestMapping(value = "/createTable" ,method = RequestMethod.GET)
    public void  createTable(){
//        String sql = "CREATE TABLE "+ DalConstant.DATABASE+".DEPARTMENT_ORDER_INFO_partition (ID varchar(255),DEPARTMENT_ID BIGINT,ORDER_ID varchar(255), PASSENGER_ID BIGINT,PASSENGER_TYPE varchar(255),validTime string,TMC_ID BIGINT,COMPANY_ID BIGINT,BLOODRELATION int,itemId BIGINT,orderCreateTime string, topic string,messageId String) partitioned by (dateTime string,order_status int) row format delimited fields terminated by '\t'";
        String sql = "CREATE TABLE "+ DalConstant.DATABASE+".DEPARTMENT_ORDER_INFO_partition (ORIGINAL_DEPARTMENT_ID BIGINT, PARENT_ID BIGINT,DEPARTMENT_ID BIGINT,ORDER_ID varchar(255), PASSENGER_ID BIGINT,PASSENGER_TYPE varchar(255),validTime string,TMC_ID BIGINT,COMPANY_ID BIGINT,order_status int,BLOODRELATION int,itemId BIGINT,orderCreateTime string) partitioned by (dateTime string) row format delimited fields terminated by '\t'";
        try {
            hiveDAO.createTables(sql);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @ResponseBody
    @RequestMapping(value = "/getZk" ,method = RequestMethod.POST)
    public String  getZk(@RequestParam(value = "dataPath", required = true)  final String dataPath){

        ZookeeperExecutor zke= ZookeeperExecutor.getInstance();
        try {
           return new String(zke.getPath(dataPath));

        } catch (Exception e) {

            logger.error("<DataOperateController><getZk><getZk><getZk><getZk>" + "【获取ZK数值报错】"
                    + ",异常..........."+org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
            e.printStackTrace();
            return  null;
        }
    }


}
