/**
 * LY.com Inc.
 * Copyright (c) 2004-2016 All Rights Reserved.
 */
package com.ly.report.tmc.datacollectionapi.dal.hive.db;


import com.ly.report.tmc.datacollectionapi.dal.hive.db.to.OrderPassengerTick;
import com.ly.report.tmc.datacollectionapi.dal.hive.db.to.OrderPassengerTickUpdate;
import com.ly.report.tmc.datacollectionapi.dal.message.TopicContext;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 *
 *
 */

public interface HiveDAO {



    
    /**
     * 执行hive sql
     * 
     * @param sql
     * @return
     * @throws SQLException
     */
    List<Map<String, Object>> executeSQL(String sql, String... args) throws SQLException;
    
    /**
     * 执行hive 查询
     * @param sql
     * @return
     * @throws SQLException
     */
    List<Map<String,Object>> executeQuerySQL(String sql) throws SQLException;


    /**
     * 新建表
     * @param sql
     */
    void createTables(String sql);

    /**
     * 获取需要调用查询的次数
     * @return
     */
    List<Long> getCountTime(String tableName,String timeColumn, String...args);

    Long getTotalTime(String tableName);


    /**
     * 分批获取查询结果
     * @param pageNum
     * @return
     */
    List<Map<String, Object>> getResultByPage(int pageNum,String...args);


    /**
     * 分批获取改签查询结果
     * @param pageNum
     * @return
     */
    List<Map<String, Object>> getChangeResultByPage(int pageNum,String...args);




    /**
     * 分批获取改签查询结果
     * @param pageNum
     * @return
     */
    List<Map<String, Object>> getRefundResultByPage(int pageNum,String...args);




    /**
     * 分批获取订单好
     * @param pageNum
     * @return
     */
    List<String> getOrderNumByPage(int pageNum);


    List<OrderPassengerTick> getOrderItemByOrderNum(String orderNum);


    /**
     * 批量插入数据
     * @param args
     * @return
     */
    int[] batchInsertOrderDep(List<Object[]> args,String tableName);



    int isTopicDataExit(String...args);


    void dropTable(String tableName);

    void createTable(String tableName);


    void batchInsertOrderByPartition(Object[] args);


    void deletePartition(String partition);


    Long getUpdateCountTime(String sql, String...args);


   void getOrderPassengerTickBatch(int batchNum,String tableName,String ...args);


   void insertChangeData(int batchNum,String tableName,String ...args);

   void insertRefundData(int batchNum,String tableName,String ...args);


}
