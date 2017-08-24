package com.ly.report.tmc.datacollectionapi.biz.util;

import com.ly.report.tmc.datacollectionapi.biz.analyze.DataAnalyze;
import com.ly.report.tmc.datacollectionapi.biz.consumer.MessageConsumerSeparate;
import com.ly.report.tmc.datacollectionapi.biz.consumer.MessageConsumerSeparateUpdate;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

/**
 * 消费者启动器
 * Created by hyz46086 on 2017/5/10.
 */
public class BeanPostProcess implements ApplicationListener<ContextRefreshedEvent> {

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        if(event.getApplicationContext().getParent() == null){
            MessageConsumerSeparateUpdate messageConsumerSeparateUpdate= (MessageConsumerSeparateUpdate) Springfactory.getBean("messageConsumerSeparateUpdate",MessageConsumerSeparateUpdate.class);
            messageConsumerSeparateUpdate.process();
        }
    }
}
