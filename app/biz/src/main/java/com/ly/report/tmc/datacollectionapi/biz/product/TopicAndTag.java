package com.ly.report.tmc.datacollectionapi.biz.product;

/**
 * Created by hyz46086 on 2017/4/17.
 */
public class TopicAndTag {

    private String topic;

    private String tag;

    private String groupName;

    //信息载体
    public Object object;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public Object getObject() {
        return object;
    }

    public void setObject(Object object) {
        this.object = object;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }
}
