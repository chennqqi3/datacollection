package com.ly.report.tmc.datacollectionapi.biz.product;

import java.util.List;

/**
 * Created by hyz46086 on 2017/4/17.
 */
public class MqModel<T,M> {

    private String groupName;

    private List<T> producerList;

    private List<M> consumerList;

    private List<TopicAndTag> topicAndTagList;

    public List<TopicAndTag> getTopicAndTagList() {
        return topicAndTagList;
    }

    public void setTopicAndTagList(List<TopicAndTag> topicAndTagList) {
        this.topicAndTagList = topicAndTagList;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public List<T> getProducerList() {
        return producerList;
    }

    public void setProducerList(List<T> producerList) {
        this.producerList = producerList;
    }

    public List<M> getConsumerList() {
        return consumerList;
    }

    public void setConsumerList(List<M> consumerList) {
        this.consumerList = consumerList;
    }
}
