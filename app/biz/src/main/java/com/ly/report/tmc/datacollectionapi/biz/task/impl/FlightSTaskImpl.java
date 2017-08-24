package com.ly.report.tmc.datacollectionapi.biz.task.impl;

import com.ly.report.tmc.datacollectionapi.biz.task.FlightTask;
import com.ly.report.tmc.datacollectionapi.biz.task.TaskEnum;
import com.ly.report.tmc.datacollectionapi.biz.task.TaskFactory;
import com.ly.report.tmc.datacollectionapi.dal.daointerface.TaskDAO;
import com.ly.report.tmc.datacollectionapi.dal.dataobject.TaskDO;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Date;

/**
 * Created by hyz46086 on 2017/4/21.
 */
@Service
public class FlightSTaskImpl implements TaskFactory<FlightTask>{
    @Resource
    TaskDAO taskDAO;

    public  static  FlightTask FLIGHT_S_TASK = null;
    @Override
    public synchronized  FlightTask createTask()  {
        if(FLIGHT_S_TASK==null){
            FlightTask flightTask = new FlightTask();
            TaskDO  taskDO =  new TaskDO();
            taskDO.setCreatetime(new Date());
            taskDO.setName("报表统计任务");
            taskDO.setDescription("报表统计任务");
            taskDO.setState(TaskEnum.START.getCode());
            flightTask.setTaskDO(taskDO);
            //todo 任务入库
            return  flightTask;
        }
        return FLIGHT_S_TASK;
    }
}
