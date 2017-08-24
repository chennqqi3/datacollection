package com.ly.report.tmc.datacollectionapi.biz.listener;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.ly.report.tmc.datacollectionapi.biz.analyze.DataAnalyzeUpdate;
import com.ly.report.tmc.datacollectionapi.biz.collection.impl.DataCollectionServiceImpl;
import com.ly.report.tmc.datacollectionapi.biz.common.Constant;
import com.ly.report.tmc.datacollectionapi.biz.util.Springfactory;
import com.ly.report.tmc.datacollectionapi.biz.zk.impl.PreliminaryNodeChildrenCallback;
import com.ly.report.tmc.datacollectionapi.biz.analyze.DataAnalyze;
import com.ly.report.tmc.datacollectionapi.biz.zk.ZKNodeStatus;
import com.ly.report.tmc.datacollectionapi.biz.zk.ZookeeperExecutor;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * Created by hyz46086 on 2017/4/17.
 */

public class MessageListener implements MessageListenerConcurrently {


    /** logger */
    private Logger logger = LoggerFactory.getLogger(MessageListener.class);
    private static volatile MessageListener messageListener;

    private DataAnalyzeUpdate dataAnalyze = (DataAnalyzeUpdate) Springfactory.getBean("dataAnalyzeUpdate",DataAnalyzeUpdate.class);
    public   MessageListener(){
    }
    public static MessageListener getInstance(){
        if (messageListener == null) {
            synchronized (MessageListener.class) {
                if (messageListener == null) {
                    messageListener = new MessageListener();
                }
            }
        }
        return messageListener;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext consumeConcurrentlyContext) {

            try {

                for (MessageExt messageExt : msgs) {
                    dataAnalyze.analyze(messageExt);
                    }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }catch (Exception e){
                e.printStackTrace();
                logger.error("<reportcollectionapi><MessageListener><consumeMessage><consumeMessage><consumeMessage>" + "【消费异常】消费异常，入参=" + msgs.toString()
                        + ",异常********************************" +org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));

                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
    }
}
