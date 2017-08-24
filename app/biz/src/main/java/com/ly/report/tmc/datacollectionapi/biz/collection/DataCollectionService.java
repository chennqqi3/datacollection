package com.ly.report.tmc.datacollectionapi.biz.collection;

import com.ly.report.tmc.datacollectionapi.biz.exception.BizException;
import com.ly.report.tmc.datacollectionapi.biz.task.FlightTask;
import com.ly.report.tmc.datacollectionapi.biz.zk.ZookeeperExecutor;

/**
 * Created by hyz46086 on 2017/4/17.
 */

public interface DataCollectionService {

 /**
  * 启动发送统计任务
  */
 void startJob(String startTime,String endTime);

 /**
  *生产发送处理数据
  */
 void produceAndSendMessage(FlightTask flightTask) throws BizException;


 /**
  * 生产改签数据
  * @param flightTask
  * @throws BizException
  */
 void produceChangeMessage(FlightTask flightTask) throws BizException;


 /**
  * 生产退票数据
  * @param flightTask
  * @throws BizException
  */
 void produceRefundMessage(FlightTask flightTask) throws BizException;


}
