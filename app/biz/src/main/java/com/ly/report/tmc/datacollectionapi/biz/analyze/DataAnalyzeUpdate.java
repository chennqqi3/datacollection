/**
 * LY.com Inc.
 * Copyright (c) 2004-2017 All Rights Reserved.
 */
package com.ly.report.tmc.datacollectionapi.biz.analyze;

import java.util.Date;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.Resource;

import com.ly.report.tmc.datacollectionapi.biz.task.RequsetDtoFactory;
import com.ly.report.tmc.datacollectionapi.biz.zk.ZookeeperExecutor;
import com.ly.report.tmc.datacollectionapi.biz.zk.impl.PreliminaryNodeChildrenCallback;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.ly.flight.tmc.reportanalysisapi.facade.dto.RequestDTO;
import com.ly.report.tmc.datacollectionapi.biz.redis.RedisUtils;
import com.ly.report.tmc.datacollectionapi.biz.util.StringHelper;
import com.ly.report.tmc.datacollectionapi.dal.hive.db.ChangeHiveDAO;
import com.ly.report.tmc.datacollectionapi.dal.hive.db.HiveDAO;
import com.ly.report.tmc.datacollectionapi.dal.hive.db.RefundHiveDAO;

/**
 * @author hyz46086
 * @version $Id: DataAnalyzeUpdate, v 0.1 2017/5/26 11:36 hyz46086 Exp $
 */
@Service
public class DataAnalyzeUpdate implements DataAnalyzeUpdateService {

    @Resource
    private HiveDAO              hiveDAO;

    @Resource
    private ChangeHiveDAO        changeHiveDAO;

    @Resource
    private RefundHiveDAO        refundHiveDAO;

    private String               assign_topic;

    private String               primitive_topic;

    private String               change_topic;

    private String               refund_topic;

    private String               message_group;

    @Resource
    DoInvoke                     doInvoke;
    /**
     * logger
     */
    private Logger               logger   = LoggerFactory.getLogger(DataAnalyzeUpdate.class);

    CopyOnWriteArrayList<String> taskList = ListService.getSafeList();

    @Override
    public void analyze(MessageExt messageExt) {

        logger.info("<reportcollectionapi><DataAnalyze><analyze><analyzeStart><" + messageExt.getMsgId() + ">【analyze数据分析】数据分析开始执行，入参=" + messageExt.toString() + ",当前时间**********"
                    + new Date().getTime() + "**********************" + Thread.currentThread().getName() + "消息体：：：：" + JSONObject.parseObject(new String(messageExt.getBody())));
        String topic = messageExt.getTopic();
        String message = new String(messageExt.getBody());
        JSONObject temp = JSONObject.parseObject(message);
//        RequestDTO requestDTO = new RequestDTO();
//        requestDTO.setEndDate(temp.getString("endTime"));
//        requestDTO.setStartDate(temp.getString("startTime"));


        if (assign_topic.equals(topic)) {
            String taskName = JSONObject.parseObject(message).getString("name");
            RequsetDtoFactory.getRequestDTO(JSONObject.parseObject(message).getString("startTime"),JSONObject.parseObject(message).getString("endTime"));
            taskList.add(taskName);
            logger.info("<reportcollectionapi><DataAnalyze><analyze><analyze><getNowListSize>【根据页数查询数据】根据页数查询数据，入参=" + "开始查看taskList的值：：：：：：" + taskList.size()
                        + "当前收的topic值cplist:::::" + taskName + "::::" + JSON.toJSONString(taskList));

        }

        if (primitive_topic.equals(topic)) {
            //设置时间
            RequestDTO requestDTO= RequsetDtoFactory.getRequestDTO(temp.getString("startTime"),temp.getString("endTime"));
            try {
                logger.info("<reportcollectionapi><DataAnalyze><createTopic><findData><" + messageExt.getMsgId() + ">【根据页数查询数据】根据页数查询数据，入参=" + messageExt.toString()
                            + "开始查询时间：：：：：：" + new Date().getTime());
                hiveDAO.getOrderPassengerTickBatch(temp.getIntValue("batchNum"), "Temporary_" + StringHelper.deleteLine(temp.getString("startTime").split(" ")[0]),
                    new String[] { temp.getString("startTime"), temp.getString("endTime") });
                logger.info("<reportcollectionapi><DataAnalyze><createTopic><findData><" + messageExt.getMsgId() + ">【根据页数查询数据】根据页数查询数据，入参=" + messageExt.toString()
                            + "结束查询时间：：：：：：" + new Date().getTime());
                RedisUtils.incrKey("primitiveFlag", 1);
                doInvoke.doInvoke(requestDTO, taskList);
            } catch (Exception e) {
                logger.error("<reportcollectionapi><DataAnalyze><analyze><analyze><dealPrimitiveOrder>" + "【处理原始数据出错】处理原始数据出错，入参=" + messageExt.toString() + ",异常订单号.............."
                             + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
                e.printStackTrace();
            }
        }
        if (change_topic.equals(topic)) {
            //设置时间
            RequestDTO requestDTO= RequsetDtoFactory.getRequestDTO(temp.getString("startTime"),temp.getString("endTime"));
            try {
                logger.info("<reportcollectionapi><DataAnalyze><changeTopic><findData><" + messageExt.getMsgId() + ">【根据页数查询数据】根据页数查询数据，入参=" + messageExt.toString()
                            + "开始查询时间：：：：：：" + new Date().getTime());
                hiveDAO.insertChangeData(temp.getIntValue("batchNum"), "Temporary_" + StringHelper.deleteLine(temp.getString("startTime").split(" ")[0]),
                    new String[] { temp.getString("startTime"), temp.getString("endTime") });
                logger.info("<reportcollectionapi><DataAnalyze><changeTopic><findData><" + messageExt.getMsgId() + ">【根据页数查询数据】根据页数查询数据，入参=" + messageExt.toString()
                            + "结束查询时间：：：：：：" + new Date().getTime());
                RedisUtils.incrKey("changeFlag", 1);
                doInvoke.doInvoke(requestDTO, taskList);
            } catch (Exception e) {
                logger.error("<reportcollectionapi><DataAnalyze><analyze><analyze><dealChangeOrder>" + "【处理改签数据出错】处理改签数据出错，入参=" + messageExt.toString() + ",异常订单号.............."
                             + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
                e.printStackTrace();
            }
        }

        if (refund_topic.equals(topic)) {
            //设置时间
            RequestDTO requestDTO= RequsetDtoFactory.getRequestDTO(temp.getString("startTime"),temp.getString("endTime"));
            try {
                logger.info("<reportcollectionapi><DataAnalyze><refundTopic><findData><" + messageExt.getMsgId() + ">【根据页数查询数据】根据页数查询数据，入参=" + messageExt.toString()
                            + "开始查询时间：：：：：：" + new Date().getTime());
                hiveDAO.insertRefundData(temp.getIntValue("batchNum"), "Temporary_" + StringHelper.deleteLine(temp.getString("startTime").split(" ")[0]),
                    new String[] { temp.getString("startTime"), temp.getString("endTime") });
                logger.info("<reportcollectionapi><DataAnalyze><refundTopic><findData><" + messageExt.getMsgId() + ">【根据页数查询数据】根据页数查询数据，入参=" + messageExt.toString()
                            + "结束查询时间：：：：：：" + new Date().getTime());
                RedisUtils.incrKey("refundFlag", 1);
                doInvoke.doInvoke(requestDTO, taskList);
            } catch (Exception e) {
                logger.error("<reportcollectionapi><DataAnalyze><analyze><analyze><dealChangeOrder>" + "【处理退票数据出错】处理退票数据出错，入参=" + messageExt.toString() + ",异常订单号.............."
                             + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
                e.printStackTrace();
            }
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