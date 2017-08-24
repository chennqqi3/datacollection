/**
 * LY.com Inc.
 * Copyright (c) 2004-2017 All Rights Reserved.
 */
package com.ly.report.tmc.datacollectionapi.biz.consumer;

import com.ly.report.tmc.datacollectionapi.biz.zk.ZookeeperExecutor;
import com.ly.report.tmc.datacollectionapi.biz.zk.impl.PreliminaryNodeChildrenCallback;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.ly.report.tmc.datacollectionapi.biz.listener.MessageListener;

import javax.annotation.Resource;

/**
 * @author hyz46086
 * @version $Id: MessageConsumerSeparateUpdate, v 0.1 2017/5/26 11:24 hyz46086 Exp $
 */
@Service
public class MessageConsumerSeparateUpdate {

    /** logger */
    private Logger                      logger   = LoggerFactory.getLogger(MessageConsumerSeparate.class);

    public static DefaultMQPushConsumer consumer = null;

    @Resource
    private PreliminaryNodeChildrenCallback preliminaryNodeChildrenCallback;

    public static boolean               flag;

    private String                      assign_topic;

    private String                      primitive_topic;

    private String                      change_topic;

    private String                      refund_topic;

    private String                      message_group;

    private String                      nameSrvAddress;

    /**
     * 启动任务消费
     * @param
     * @return
     */
    public String process() {
        String result = "error";
        String errMsg = new String();
        byte isErr = 0;
        try {
            synchronized (MessageConsumer.class) {
                if (consumer == null) {
                    consumer = new DefaultMQPushConsumer(message_group);
                    consumer.setNamesrvAddr(nameSrvAddress);
                    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
                    consumer.setConsumeThreadMax(20);
                    consumer.subscribe(primitive_topic, "*");
                    consumer.subscribe(change_topic, "*");
                    consumer.subscribe(refund_topic, "*");
                    consumer.subscribe(assign_topic, "*");
                    consumer.registerMessageListener(MessageListener.getInstance());
                }
                if (!flag) {
                    consumer.start();
                    logger.info("<ReportCollectionApi><MessageConsumerSeparateUpdate><process><startConsumer><startConsumer>" + "【启动consumer】启动consumer，入参=nothing"
                                + ",结果=启动consumer.............................");
                    //注册zk 监听器
                    ZookeeperExecutor.getInstance().watchChildrenPath("/TCTMCstatisticsNode", preliminaryNodeChildrenCallback, PathChildrenCacheEvent.Type.CHILD_UPDATED);
                    flag = true;
                }
            }
        } catch (MQClientException e) {
            isErr = 1;
            logger.error("<reportcollectionapi><MessageConsumerSeparate><process><consumer><consumer>" + "【消费失败】消费失败，入参=nothing" + ",异常="
                         + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
            errMsg = e.getErrorMessage();
        } catch (Exception e) {
            e.printStackTrace();
            isErr = 1;
            logger.error("<reportcollectionapi><MessageConsumerSeparate><process><consumer><consumer>" + "【消费失败】消费失败，入参=nothing" + ",异常="
                         + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
            errMsg = e.getMessage();
        }
        if (isErr == 1) {
            result = errMsg;
        }
        return result;
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

    public String getNameSrvAddress() {
        return nameSrvAddress;
    }

    public void setNameSrvAddress(String nameSrvAddress) {
        this.nameSrvAddress = nameSrvAddress;
    }

}
