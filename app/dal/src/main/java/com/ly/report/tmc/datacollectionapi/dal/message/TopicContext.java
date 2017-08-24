package com.ly.report.tmc.datacollectionapi.dal.message;

/**
 *
 * Created by hyz46086 on 2017/5/5.
 */
public class TopicContext {

    private  String startTime;

    private String endTime;

    private  String topic;

    private  int orderType;

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getOrderType() {
        return orderType;
    }

    public void setOrderType(int orderType) {
        this.orderType = orderType;
    }
}
