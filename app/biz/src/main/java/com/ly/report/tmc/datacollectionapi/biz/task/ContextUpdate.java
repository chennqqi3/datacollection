/**
 * LY.com Inc.
 * Copyright (c) 2004-2017 All Rights Reserved.
 */
package com.ly.report.tmc.datacollectionapi.biz.task;

/**
 * @author hyz46086
 * @version $Id: ContextUpdate, v 0.1 2017/5/26 10:43 hyz46086 Exp $
 */
public class ContextUpdate
{
    //开始时间
    private  String startTime;

    //结束时间
    private String endTime;

    //处理批号
    private int batchNum;

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

    public int getBatchNum() {
        return batchNum;
    }

    public void setBatchNum(int batchNum) {
        this.batchNum = batchNum;
    }
}
