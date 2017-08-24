/**
 * LY.com Inc.
 * Copyright (c) 2004-2017 All Rights Reserved.
 */
package com.ly.report.tmc.datacollectionapi.biz.collection.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Resource;

import com.ly.report.tmc.datacollectionapi.biz.analyze.ListService;
import com.ly.report.tmc.datacollectionapi.biz.task.RequsetDtoFactory;
import com.ly.report.tmc.datacollectionapi.biz.zk.ZookeeperExecutor;
import com.ly.report.tmc.datacollectionapi.biz.zk.impl.PreliminaryNodeChildrenCallback;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ly.report.tmc.datacollectionapi.biz.analyze.ReportAnalysisEnum;
import com.ly.report.tmc.datacollectionapi.biz.collection.DataCollectionUpdateService;
import com.ly.report.tmc.datacollectionapi.biz.collection.TaskMessageBody;
import com.ly.report.tmc.datacollectionapi.biz.common.Constant;
import com.ly.report.tmc.datacollectionapi.biz.exception.BizException;
import com.ly.report.tmc.datacollectionapi.biz.product.MessageProduct;
import com.ly.report.tmc.datacollectionapi.biz.product.TopicAndTag;
import com.ly.report.tmc.datacollectionapi.biz.redis.RedisUtils;
import com.ly.report.tmc.datacollectionapi.biz.task.ContextUpdate;
import com.ly.report.tmc.datacollectionapi.biz.task.TaskFactory;
import com.ly.report.tmc.datacollectionapi.biz.util.StringHelper;
import com.ly.report.tmc.datacollectionapi.dal.common.DalConstant;
import com.ly.report.tmc.datacollectionapi.dal.hive.db.HiveDAO;

/**
 * @author hyz46086
 * @version $Id: DataCollectionUpdateServiceImpl, v 0.1 2017/5/26 10:31 hyz46086 Exp $
 */
public class DataCollectionUpdateServiceImpl implements DataCollectionUpdateService {
    /** logger */
    private Logger logger = LoggerFactory.getLogger(DataCollectionServiceImpl.class);


/*
    @Resource
    private PreliminaryNodeChildrenCallback preliminaryNodeChildrenCallback;*/

    @Resource
    TaskFactory    taskFactory;

    @Resource
    HiveDAO        hiveDAO;

    @Resource
    MessageProduct messageProduct;

    private String assign_topic;

    private String primitive_topic;

    private String change_topic;

    private String refund_topic;

    private String message_group;

    @Override
    public void startJob(String startTime, String endTime) {

        try {
            logger.info("<ReportCollectionApi><DataCollectionUpdateServiceImpl><startJob><startJob><startJob>" + "【任务启动】启动任务，入参=" + startTime + endTime + ",结果=");
            //创建上下文环境
            ContextUpdate contextUpdate = new ContextUpdate();
            contextUpdate.setStartTime(startTime);
            contextUpdate.setEndTime(endTime);

            //删除创建临时表
            String tableName = "Temporary_" + StringHelper.deleteLine(startTime.split(" ")[0]);
            hiveDAO.dropTable(tableName);
            hiveDAO.createTable(tableName);
            // 初始化zk,启动zk服务
            ZookeeperExecutor zookeeperExecutor = ZookeeperExecutor.getInstance();
            //结束標記归零
            zookeeperExecutor.setPath("/TCTMCstatisticsNode/OverFlag","0");
            //启动多任务线程
            ExecutorService pool = Executors.newFixedThreadPool(6);
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    //生产发送调度接口
                    try {
                        assignTask(contextUpdate);
                    } catch (BizException e) {
                        e.printStackTrace();
                    }
                }
            });

            pool.execute(new Runnable() {
                @Override
                public void run() {
                    //生产发送原始订单批号
                    try {
                        productAndsendPrimitiveBatchNum(contextUpdate);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            pool.execute(new Runnable() {
                @Override
                public void run() {
                    //生产发送改签订单批号
                    try {
                        productAndsendChangeBatchNum(contextUpdate);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            pool.execute(new Runnable() {
                @Override
                public void run() {
                    //生产发送退票订单批号
                    try {
                        productAndsendRefundBatchNum(contextUpdate);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            pool.shutdown();

        } catch (Exception e) {
            logger.error("<reportcollectionapi><startJob><startJob><startJob><startJob>" + "【任务启动】启动任务，入参=" + startTime+endTime
                    + ",结果=" +  org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
            e.printStackTrace();
        }
    }

    public void assignTask(ContextUpdate contextUpdate) throws BizException {
        try {
            for (ReportAnalysisEnum reportAnalysisEnum : ReportAnalysisEnum.values()) {
                TopicAndTag topicAndTag = new TopicAndTag();
                topicAndTag.setTopic(assign_topic);
                topicAndTag.setTag(assign_topic);
                TaskMessageBody taskMessageBody = new TaskMessageBody();
                taskMessageBody.setName(reportAnalysisEnum.getKey());
                taskMessageBody.setCode(reportAnalysisEnum.getValue());
                taskMessageBody.setStartTime(contextUpdate.getStartTime());
                taskMessageBody.setEndTime(contextUpdate.getEndTime());
                topicAndTag.setObject(taskMessageBody);
                topicAndTag.setGroupName(message_group);
                messageProduct.process(topicAndTag);
            }
        } catch (Exception e) {
            logger.error("<reportcollectionapi><DataCollectionServiceImpl><assignTask><assignTask><assignTask>" + "【生产者生产数据】生产者生产数据，入参=" + JSON.toString(contextUpdate)
                         + ",生产信息异常............" + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
        }
    }

    private void productAndsendPrimitiveBatchNum(ContextUpdate contextUpdate) {
        try{
            String sql = " select count(DISTINCT foi.PASSENGER_ID) as allcount FROM " + DalConstant.DATABASE + ".FLIGHT_ORDER_ITEM as foi  LEFT JOIN " + DalConstant.DATABASE
                    + ".PASSENGER as pa  on  foi.PASSENGER_ID = pa.ID LEFT JOIN " + DalConstant.DATABASE + ".FLIGHT_ORDER as fo on foi.FLIGHT_ORDER_NO = fo.ORDER_NO  LEFT JOIN  "
                    + DalConstant.DATABASE + ".FLIGHT_TICKET as ft ON foi.FLIGHT_TICKET_ID = ft.ID where foi.ITEM_TYPE=0 and fo.OUT_TICKET_TIME between ? and ?";
            Long totalBatch = hiveDAO.getUpdateCountTime(sql, new String[] { contextUpdate.getStartTime(), contextUpdate.getEndTime() });
            RedisUtils.set("primitiveFlag", "0", Constant.VALID_TIME);
            if (totalBatch != null && totalBatch > 0) {
                RedisUtils.set("primitiveTotalBatch", totalBatch + "", Constant.VALID_TIME);
                for (int i = 1; i <= totalBatch; i++){
                    TopicAndTag topicAndTag = new TopicAndTag();
                    topicAndTag.setTopic(primitive_topic);
                    topicAndTag.setTag(primitive_topic);
                    contextUpdate.setBatchNum(i);
                    topicAndTag.setObject(contextUpdate);
                    topicAndTag.setGroupName(message_group);
                    messageProduct.process(topicAndTag);
                }
            } else {
                RedisUtils.set("primitiveTotalBatch", "0", Constant.VALID_TIME);
            }

        }catch (Exception e){
            logger.error("<reportcollectionapi><DataCollectionServiceImpl><primitiveTask><primitiveTask><primitiveTask>" + "【生产者生产数据】生产者生产数据，入参=" + JSON.toString(contextUpdate)
                    + ",生产信息异常............" + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
        }

    }

    private void productAndsendChangeBatchNum(ContextUpdate contextUpdate) {
        try{
            String sql = "select count(1) as allcount from " + DalConstant.DATABASE + ".FLIGHT_CHANGE_APPLY_ITEM as fcai LEFT JOIN " + DalConstant.DATABASE
                    + ".FLIGHT_CHANGE_APPLY as fca ON fcai.APPLY_NO=fca.APPLY_NO LEFT JOIN " + DalConstant.DATABASE
                    + ".FLIGHT_ORDER_ITEM as foi ON fcai.NEW_ORDER_ITEM_ID = foi.ID LEFT JOIN " + DalConstant.DATABASE
                    + ".PASSENGER as pa on foi.PASSENGER_ID = pa.ID where fca.CHANGE_STATUS IN ('01','06',07) and fca.CHANGE_FINISH_DATE between ? and ?";
            Long totalBatch = hiveDAO.getUpdateCountTime(sql, new String[] { contextUpdate.getStartTime(), contextUpdate.getEndTime() });
            RedisUtils.set("changeFlag", "0", Constant.VALID_TIME);
            if (totalBatch != null && totalBatch > 0) {
                RedisUtils.set("changeTotalBatch", totalBatch + "", Constant.VALID_TIME);
                for (int i = 1; i <= totalBatch; i++) {
                    TopicAndTag topicAndTag = new TopicAndTag();
                    topicAndTag.setTopic(change_topic);
                    topicAndTag.setTag(change_topic);
                    contextUpdate.setBatchNum(i);
                    topicAndTag.setObject(contextUpdate);
                    topicAndTag.setGroupName(message_group);
                    messageProduct.process(topicAndTag);
                }
            } else {
                RedisUtils.set("changeTotalBatch", "0", Constant.VALID_TIME);
            }

        }catch (Exception e){
            logger.error("<reportcollectionapi><DataCollectionServiceImpl><changeTask><changeTask><changeTask>" + "【生产者生产数据】生产者生产数据，入参=" + JSON.toString(contextUpdate)
                    + ",生产信息异常............" + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
        }

    }

    private void productAndsendRefundBatchNum(ContextUpdate contextUpdate) {
        try{
            String sql = "SELECT count(1) as allcount from " + DalConstant.DATABASE + ".FLIGHT_REFUND_APPLY_ITEM as frai LEFT JOIN " + DalConstant.DATABASE
                    + ".FLIGHT_REFUND_APPLY as fra ON frai.FLIGHT_REFUND_APPLY_NO = fra.APPLY_NO LEFT JOIN " + DalConstant.DATABASE
                    + ".FLIGHT_ORDER_ITEM as foi ON frai.ORDER_ITEM_ID=foi.ID LEFT JOIN " + DalConstant.DATABASE
                    + ".PASSENGER as pa on foi.PASSENGER_ID=pa.ID where fra.REFUND_STATUS IN ('02','03') and fra.REFUND_FINISH_DATE between ? and ?";
            Long totalBatch = hiveDAO.getUpdateCountTime(sql, new String[] { contextUpdate.getStartTime(), contextUpdate.getEndTime() });
            RedisUtils.set("refundFlag", "0", Constant.VALID_TIME);
            if (totalBatch != null && totalBatch > 0) {
                RedisUtils.set("refundTotalBatch", totalBatch + "", Constant.VALID_TIME);
                for (int i = 1; i <= totalBatch; i++) {
                    TopicAndTag topicAndTag = new TopicAndTag();
                    topicAndTag.setTopic(refund_topic);
                    topicAndTag.setTag(refund_topic);
                    contextUpdate.setBatchNum(i);
                    topicAndTag.setObject(contextUpdate);
                    topicAndTag.setGroupName(message_group);
                    messageProduct.process(topicAndTag);
                }
            } else {
                RedisUtils.set("refundTotalBatch", "0", Constant.VALID_TIME);
            }

        }catch (Exception e){
            logger.error("<reportcollectionapi><DataCollectionServiceImpl><refundTask><refundTask><refundTask>" + "【生产者生产数据】生产者生产数据，入参=" + JSON.toString(contextUpdate)
                    + ",生产信息异常............" + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
        }
    }

    public String getAssign_topic() {
        return assign_topic;
    }

    public void setAssign_topic(String assign_topic) {
        this.assign_topic = assign_topic;
    }

    public String getPrimitive_topic() {
        return primitive_topic;
    }

    public void setPrimitive_topic(String primitive_topic) {
        this.primitive_topic = primitive_topic;
    }

    public String getChange_topic() {
        return change_topic;
    }

    public void setChange_topic(String change_topic) {
        this.change_topic = change_topic;
    }

    public String getRefund_topic() {
        return refund_topic;
    }

    public void setRefund_topic(String refund_topic) {
        this.refund_topic = refund_topic;
    }

    public String getMessage_group() {
        return message_group;
    }

    public void setMessage_group(String message_group) {
        this.message_group = message_group;
    }

}
