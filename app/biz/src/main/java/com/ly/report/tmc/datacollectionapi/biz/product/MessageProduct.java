package com.ly.report.tmc.datacollectionapi.biz.product;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.ly.report.tmc.datacollectionapi.biz.util.StringHelper;

/**
 * Created by hyz46086 on 2017/4/17.
 */
@Service
public class MessageProduct {

    /** logger */
    private Logger                           logger   = LoggerFactory.getLogger(MessageProduct.class);

    public static volatile DefaultMQProducer producer = null;

    public static boolean                    flag     = false;

    private String                           nameSrvAddress;

    /**
     * 生产者 生产数据
     * @param topicATag
     * @return
     */
    public String process(TopicAndTag topicATag) {

        String errMsg = new String();
        SendResult sendResult = null;
        if (topicATag != null) {
            if (StringHelper.isNullOrEmpty(topicATag.getTopic()) || StringHelper.isNullOrEmpty(topicATag.getTag())) {
                errMsg = "请确认topic或Tag是否为空";
            } else {
                try {
                    synchronized (MessageProduct.class) {
                        if (producer == null) {
                            producer = new DefaultMQProducer(topicATag.getGroupName());//创建生产者，my_group_name是用来标识消息生产者的组名，生产者的组名只用于在后台区分实例是哪个应用启的，没有其它作用。
                            producer.setNamesrvAddr(nameSrvAddress);
                            producer.setSendMsgTimeout(500000);
                            if (!flag) {
                                producer.start();
                                flag = true;
                            }
                        }
                    }
                    Message msg = new Message(topicATag.getTopic(), // topic
                        topicATag.getTag(), // tag
                        JSON.toJSONString(topicATag.getObject()).getBytes("UTF-8")// body
                    );
                    sendResult = producer.send(msg);
                } catch (MQClientException e) {
                    logger.error("<reportcollectionapi><MessageProduct><process><process><process>" + "【生产者】启动任务，入参=" + org.mortbay.util.ajax.JSON.toString(topicATag)

                                 + ",异常=" + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
                    errMsg = e.getErrorMessage();
                } catch (InterruptedException e) {

                    logger.error("<reportcollectionapi><MessageProduct><process><process><process>" + "【任务启动】启动任务，入参=" + org.mortbay.util.ajax.JSON.toString(topicATag) + ",异常="
                                 + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
                    errMsg = e.getLocalizedMessage();
                } catch (Exception e) {
                    logger.error("<reportcollectionapi><MessageProduct><process><process><process>" + "【任务启动】启动任务，入参=" + org.mortbay.util.ajax.JSON.toString(topicATag)

                                 + ",异常=" + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
                    e.printStackTrace();
                    errMsg = e.getMessage();
                }
            }
        }
        return JSON.toJSONString(sendResult);
    }

    public String getNameSrvAddress() {
        return nameSrvAddress;
    }

    public void setNameSrvAddress(String nameSrvAddress) {
        this.nameSrvAddress = nameSrvAddress;
    }

}
