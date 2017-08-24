package com.ly.report.tmc.datacollectionapi.biz.consumer;

import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.ly.report.tmc.datacollectionapi.biz.analyze.DataAnalyze;
import com.ly.report.tmc.datacollectionapi.biz.collection.impl.DataCollectionServiceImpl;
import com.ly.report.tmc.datacollectionapi.biz.common.Constant;
import com.ly.report.tmc.datacollectionapi.biz.listener.MessageListener;
import com.ly.report.tmc.datacollectionapi.biz.product.MqModel;
import com.ly.report.tmc.datacollectionapi.biz.product.TopicAndTag;
import com.ly.report.tmc.datacollectionapi.biz.util.StringHelper;
import com.ly.report.tmc.datacollectionapi.biz.zk.ZookeeperExecutor;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * Created by hyz46086 on 2017/5/10.
 */
@Service
public class MessageConsumerSeparate {


    /** logger */
    private Logger logger = LoggerFactory.getLogger(MessageConsumerSeparate.class);

    public  static DefaultMQPushConsumer consumer =null;

    public  static  boolean flag;

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
                        consumer = new DefaultMQPushConsumer(Constant.MESSAGE_GROUP);
                        consumer.setNamesrvAddr(Constant.MQ_ADDRESS);
                        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
                        consumer.setConsumeThreadMax(30);
                        consumer.subscribe(Constant.ASSIGN_TOPIC, "*");
                        consumer.subscribe(Constant.REFUND_TOPIC, "*");
                        consumer.subscribe(Constant.CHANGE_TOPIC, "*");
                        consumer.subscribe(Constant.CREATE_TOPIC, "*");
                        consumer.registerMessageListener(MessageListener.getInstance());
                    }
                    if (!flag) {
                        consumer.start();
                        logger.info("<reportcollectionapi><MessageConsumerSeparate><process><startConsumer><startConsumer>" + "【启动consumer】启动consumer，入参=nothing" + ",结果=启动consumer............................." );
                        System.out.print("启动consumer.............................");
                        flag = true;
                    }
                }

        } catch (MQClientException e) {
            isErr = 1;
            logger.error("<reportcollectionapi><MessageConsumerSeparate><process><consumer><consumer>" + "【消费失败】消费失败，入参=nothing" + ",异常=" +org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
            errMsg = e.getErrorMessage();
        } catch (Exception e) {
            e.printStackTrace();
            isErr = 1;
            logger.error("<reportcollectionapi><MessageConsumerSeparate><process><consumer><consumer>" + "【消费失败】消费失败，入参=nothing" + ",异常=" +org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
            errMsg = e.getMessage();
        }
        if (isErr == 1) {
            result = errMsg;
        }
        return result;
    }
}
