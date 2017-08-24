package com.ly.report.tmc.datacollectionapi.biz.task;

import com.ly.report.tmc.datacollectionapi.dal.dataobject.TaskDO;

/**
 * Created by hyz46086 on 2017/4/21.
 */
public class FlightTask {

    //执行上下文
    Context context;

    //执行任务
    TaskDO  taskDO;

    public Context getContext() {
        return context;
    }

    public void setContext(Context context) {
        this.context = context;
    }

    public TaskDO getTaskDO() {
        return taskDO;
    }

    public void setTaskDO(TaskDO taskDO) {
        this.taskDO = taskDO;
    }
}
