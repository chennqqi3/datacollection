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

import java.util.List;

/**
 * Created by hyz46086 on 2017/5/7.
 */
@Service
public class MessageDifferentConsumer {

    public  static DefaultMQPushConsumer consumer =null;
    public  static DefaultMQPushConsumer refund_consumer =null;
    public  static DefaultMQPushConsumer change_consumer =null;



    public  static boolean flag =false;
    public  static boolean refund_flag =false;
    public  static boolean change_flag =false;
    /**
     * 启动任务消费
     * @param modelList
     * @return
     */
    public String process(List<MqModel> modelList, ZookeeperExecutor zookeeperExecutor, DataAnalyze dataAnalyze) {
//    public String process(List<MqModel> modelList) {
        String result = "error";
        String errMsg = new String();
        byte isErr = 0;

        try {
            for (MqModel model : modelList) {
                if(model.getGroupName()== Constant.MESSAGE_GROUP && consumer==null ){
                    consumer = new DefaultMQPushConsumer(model.getGroupName());
                    startDiffentConsume(zookeeperExecutor, dataAnalyze, model,consumer);
                }
                if(model.getGroupName()==Constant.MESSAGE_GROUP && consumer!=null ){
                    startDiffentConsume(zookeeperExecutor, dataAnalyze, model,consumer);
                }
                if(model.getGroupName()==Constant.REFUND_MESSAGE_GROUP && refund_consumer==null ){
                    refund_consumer = new DefaultMQPushConsumer(model.getGroupName());
                    startDiffentConsume(zookeeperExecutor, dataAnalyze, model,refund_consumer);
                }
                if(model.getGroupName()==Constant.REFUND_MESSAGE_GROUP && refund_consumer!=null ){
                    startDiffentConsume(zookeeperExecutor, dataAnalyze, model,refund_consumer);
                }

                if(model.getGroupName()==Constant.CHANGE_MESSAGE_GROUP && change_consumer==null ){
                    change_consumer = new DefaultMQPushConsumer(model.getGroupName());
                    startDiffentConsume(zookeeperExecutor, dataAnalyze, model,change_consumer);
                }
                if(model.getGroupName()==Constant.CHANGE_MESSAGE_GROUP && change_consumer!=null ){
                    startDiffentConsume(zookeeperExecutor, dataAnalyze, model,change_consumer);
                }

                result = "success";
            }
        } catch (MQClientException e) {
            isErr = 1;
            errMsg = e.getErrorMessage();


        } catch (Exception e) {
            if(consumer!=null){
                consumer.shutdown();
            }
            e.printStackTrace();
            isErr = 1;
            errMsg = e.getMessage();
        }
        if (isErr == 1) {
            result = errMsg;
        }
        return result;
    }

    private void startDiffentConsume(ZookeeperExecutor zookeeperExecutor, DataAnalyze dataAnalyze, MqModel model , DefaultMQPushConsumer current_consumer) throws MQClientException {
        current_consumer.setNamesrvAddr(Constant.MQ_ADDRESS);
        current_consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        for (Object topicAndTag:model.getTopicAndTagList()) {

            if(StringHelper.isNullOrEmpty(((TopicAndTag)topicAndTag).getTag())){
                current_consumer.subscribe(((TopicAndTag)topicAndTag).getTopic(), "*");
            }else{
                current_consumer.subscribe(((TopicAndTag)topicAndTag).getTopic(), ((TopicAndTag)topicAndTag).getTag());
            }

            zookeeperExecutor.createPath("/"+Constant.ZK_NAMESPACE+"/"+((TopicAndTag)topicAndTag).getTopic(), ZKNodeStatus.REGISTER.getCode().getBytes());
        }
//        current_consumer.registerMessageListener(new MessageListener(zookeeperExecutor, dataAnalyze));
        current_consumer.registerMessageListener(MessageListener.getInstance());

        if(model.getGroupName()==Constant.MESSAGE_GROUP ){
            if(!flag){
                current_consumer.start();
                flag=true;
            }
        }
        if(model.getGroupName()==Constant.REFUND_MESSAGE_GROUP ){

            if(!refund_flag){
                current_consumer.start();
                refund_flag=true;
            }
        }

        if(model.getGroupName()==Constant.CHANGE_MESSAGE_GROUP){

            if(!change_flag){
                current_consumer.start();
                change_flag=true;
            }
        }


    }

}
