package com.ly.report.tmc.datacollectionapi.biz;


import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.ly.report.tmc.datacollectionapi.biz.consumer.MessageConsumer;
import com.ly.report.tmc.datacollectionapi.biz.product.MessageProduct;
import com.ly.report.tmc.datacollectionapi.biz.product.MqModel;
import com.ly.report.tmc.datacollectionapi.biz.product.TopicAndTag;

import java.util.*;

/**
 * Created by hyz46086 on 2017/4/17.
 */
public class TestSendMessage {

    public static void main(String args[]){

        MqModel<DefaultMQProducer,DefaultMQPushConsumer> mqmodel = new MqModel<>();
        MessageProduct messageProduct = new MessageProduct();
        mqmodel.setGroupName("test");
        List<TopicAndTag> topicAndTagList = new ArrayList<>();
//            TopicAndTag topicAndTag = new TopicAndTag();
//            topicAndTag.setTopic("topic_"+1);
//            topicAndTag.setTag(1+"");
//            List<Map<String ,Integer>> mapList = new ArrayList<>();
//            HashMap<String ,Integer> hashMap = new HashMap<>();
//            hashMap.put("testcount",20);
//            hashMap.put("testNum",12320);
//            mapList.add(hashMap);
//            topicAndTag.setObject(mapList);
//            topicAndTagList.add(topicAndTag);
//            messageProduct.process(topicAndTag);
        for(int i=0;i<6;i++){
            TopicAndTag topicAndTag = new TopicAndTag();
            topicAndTag.setTopic("topic_"+i);
            topicAndTag.setTag(i+"");
            topicAndTag.setObject(i+"");
            topicAndTagList.add(topicAndTag);
            messageProduct.process(topicAndTag);
        }
//        TopicAndTag topicAndTag = new TopicAndTag();
//        topicAndTag.setTopic("1");
//        topicAndTag.setTag("1");
//        topicAndTag.setObject("1");
//        topicAndTagList.add(topicAndTag);
//        messageProduct.process(topicAndTag,null);
//        TopicAndTag topicAndTag2 = new TopicAndTag();
//        topicAndTag2.setTopic("2");
//        topicAndTag2.setTag("2");
//        topicAndTag2.setObject("2");
//        topicAndTagList.add(topicAndTag2);
//        messageProduct.process(topicAndTag2,null);
//        TopicAndTag topicAndTagOver = new TopicAndTag();
//        topicAndTagOver.setTopic("3");
//        topicAndTagOver.setTag("3");
//        topicAndTagOver.setObject("over");
//        topicAndTagList.add(topicAndTagOver);
//        messageProduct.process(topicAndTagOver,null);
//        TopicAndTag topicAndTag4 = new TopicAndTag();
//        topicAndTag4.setTopic("4");
//        topicAndTag4.setTag("4");
//        topicAndTag4.setObject("4");
//        topicAndTagList.add(topicAndTagOver);

        mqmodel.setTopicAndTagList(topicAndTagList);
        List<MqModel> mqModels = new ArrayList<>();
        mqModels.add(mqmodel);
        MessageConsumer messageConsumer = new MessageConsumer();
//        messageConsumer.process(mqModels);

    }


}
