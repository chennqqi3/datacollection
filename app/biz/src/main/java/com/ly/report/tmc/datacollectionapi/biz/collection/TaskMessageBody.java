package com.ly.report.tmc.datacollectionapi.biz.collection;

import com.ly.report.tmc.datacollectionapi.biz.task.FlightTask;

/**
 * Created by hyz46086 on 2017/4/17.
 */
public class TaskMessageBody {

    private FlightTask flightTask;

    private  String name;

    private  String code;

    private String startTime;

    private String endTime;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public FlightTask getFlightTask() {
        return flightTask;
    }

    public void setFlightTask(FlightTask flightTask) {
        this.flightTask = flightTask;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }
}
