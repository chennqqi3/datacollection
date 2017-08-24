package com.ly.report.tmc.datacollectionapi.biz.consumer;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.ly.report.tmc.datacollectionapi.biz.analyze.DataAnalyze;
import com.ly.report.tmc.datacollectionapi.biz.common.Constant;
import com.ly.report.tmc.datacollectionapi.biz.listener.MessageListener;
import com.ly.report.tmc.datacollectionapi.biz.product.MqModel;
import com.ly.report.tmc.datacollectionapi.biz.product.TopicAndTag;
import com.ly.report.tmc.datacollectionapi.biz.util.StringHelper;
import com.ly.report.tmc.datacollectionapi.biz.zk.ZKNodeStatus;
import com.ly.report.tmc.datacollectionapi.biz.zk.ZookeeperExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * Created by hyz46086 on 2017/5/5.
 */
@Service
public class MessageConsumerByTopic {


//    public  static DefaultMQPushConsumer consumer =null;
//    public  static DefaultMQPushConsumer refund_consumer =null;
//    public  static DefaultMQPushConsumer change_consumer =null;
//    public  static boolean flag =false;
//    public  static boolean refund_flag =false;
//    public  static boolean change_flag =false;
    /**
     * 启动任务消费
     * @param modelList
     * @return
     */
    public String process(List<MqModel> modelList, ZookeeperExecutor zookeeperExecutor, DataAnalyze dataAnalyze) {

        String result = "error";
        String errMsg = new String();
        byte isErr = 0;

        try {
            for (MqModel model : modelList) {

                for (Object topicAndTag:model.getTopicAndTagList()) {

                    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(((TopicAndTag)topicAndTag).getGroupName());

                    consumer.setNamesrvAddr(Constant.MQ_ADDRESS);
                    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
                    consumer.setConsumeThreadMin(1);
                    consumer.setConsumeThreadMax(1);

                    if(StringHelper.isNullOrEmpty(((TopicAndTag)topicAndTag).getTag())){
                        consumer.subscribe(((TopicAndTag)topicAndTag).getTopic(), "*");
                    }else{
                        consumer.subscribe(((TopicAndTag)topicAndTag).getTopic(), ((TopicAndTag)topicAndTag).getTag());
                    }

                    System.out.println("chuangjian de path:"+"/"+Constant.ZK_NAMESPACE+"/"+((TopicAndTag)topicAndTag).getTopic());
                    zookeeperExecutor.createPath("/"+Constant.ZK_NAMESPACE+"/"+((TopicAndTag)topicAndTag).getTopic(), ZKNodeStatus.REGISTER.getCode().getBytes());
//                    consumer.registerMessageListener(MessageListener.getInstance());
                    consumer.registerMessageListener(MessageListener.getInstance());
                    consumer.start();

                }


                result = "success";
            }
        } catch (MQClientException e) {
            isErr = 1;
            errMsg = e.getErrorMessage();


        } catch (Exception e) {

            e.printStackTrace();
            isErr = 1;
            errMsg = e.getMessage();
        }
        if (isErr == 1) {
            result = errMsg;
        }
        return result;
    }


}
