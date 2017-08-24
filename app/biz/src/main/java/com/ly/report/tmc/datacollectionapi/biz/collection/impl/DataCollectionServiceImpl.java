package com.ly.report.tmc.datacollectionapi.biz.collection.impl;

import com.alibaba.dubbo.common.json.JSONObject;
import com.alibaba.fastjson.JSONPObject;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.ly.report.tmc.datacollectionapi.biz.analyze.DataAnalyze;
import com.ly.report.tmc.datacollectionapi.biz.analyze.ReportAnalysisEnum;
import com.ly.report.tmc.datacollectionapi.biz.collection.DataCollectionService;
import com.ly.report.tmc.datacollectionapi.biz.collection.TaskMessageBody;
import com.ly.report.tmc.datacollectionapi.biz.common.Constant;
import com.ly.report.tmc.datacollectionapi.biz.consumer.MessageConsumer;
import com.ly.report.tmc.datacollectionapi.biz.consumer.MessageConsumerByTopic;
import com.ly.report.tmc.datacollectionapi.biz.consumer.MessageDifferentConsumer;
import com.ly.report.tmc.datacollectionapi.biz.error.BizErrorFactory;
import com.ly.report.tmc.datacollectionapi.biz.exception.BizException;
import com.ly.report.tmc.datacollectionapi.biz.product.MessageProduct;
import com.ly.report.tmc.datacollectionapi.biz.product.MqModel;
import com.ly.report.tmc.datacollectionapi.biz.product.TopicAndTag;
import com.ly.report.tmc.datacollectionapi.biz.redis.RedisUtils;
import com.ly.report.tmc.datacollectionapi.biz.task.Context;
import com.ly.report.tmc.datacollectionapi.biz.task.FlightTask;
import com.ly.report.tmc.datacollectionapi.biz.task.TaskFactory;
import com.ly.report.tmc.datacollectionapi.biz.task.TaskNodeEnum;
import com.ly.report.tmc.datacollectionapi.biz.util.StringHelper;
import com.ly.report.tmc.datacollectionapi.biz.util.WebContextUtil;
import com.ly.report.tmc.datacollectionapi.biz.zk.ZKNodeStatus;
import com.ly.report.tmc.datacollectionapi.biz.zk.ZookeeperExecutor;
import com.ly.report.tmc.datacollectionapi.dal.dataobject.TaskNodeDO;
import com.ly.report.tmc.datacollectionapi.dal.hive.db.HiveDAO;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hyz46086 on 2017/4/20.
 */
@Service
public class DataCollectionServiceImpl implements DataCollectionService {


    @Resource
    private HiveDAO hiveDAO;

    @Resource
    private DataAnalyze dataAnalyze;

    @Resource
    TaskFactory taskFactory;

    @Resource
    MessageConsumer messageConsumer;

    @Resource
    MessageConsumerByTopic messageConsumerByTopic;

    @Resource
    MessageDifferentConsumer messageDifferentConsumer;

    @Resource
    MessageProduct messageProduct;


    /** logger */
    private Logger logger = LoggerFactory.getLogger(DataCollectionServiceImpl.class);


    @Override
    public void startJob(String startTime,String endTime) {


        try {
            logger.info("<reportcollectionapi><DataCollectionServiceImpl><startJob><startJob><startJob>" + "【任务启动】启动任务，入参=" + startTime+endTime
                    + ",结果=" );

            //创建上下文环境
            Context context = new Context();
            context.setStartTime(startTime);
            context.setEndTime(endTime);
            //创建任务
            FlightTask flightTask = (FlightTask) taskFactory.createTask();
            flightTask.setContext(context);

//            //删除创建临时表
            String tableName =  "Temporary_"+ StringHelper.deleteLine(startTime.split(" ")[0]);
            hiveDAO.dropTable(tableName);
            hiveDAO.createTable(tableName);

            //删除分区数据
//            hiveDAO.deletePartition(startTime.split(" ")[0]);

            ExecutorService pool = Executors.newFixedThreadPool(6);
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    //生产发送信息
                    logger.info("<reportcollectionapi><DataCollectionServiceImpl><produceAndSendMessage><produceAndSendMessage><produceAndSendMessage>" + "【生产订单信息】生产订单信息，入参=" + JSON.toString(flightTask)

                            + ",结果=生产发送信息" );
                    try {
                        produceAndSendMessage(flightTask);
                    } catch (BizException e) {
                        e.printStackTrace();
                    }
                }
            });

            pool.execute(new Runnable() {
                @Override
                public void run() {
            logger.info("<reportcollectionapi><DataCollectionServiceImpl><produceChangeMessage><produceChangeMessage><produceChangeMessage>" + "【生产改签订单信息】生产改签订单信息，入参=" + JSON.toString(flightTask)

                    + ",结果=生产改签信息" );
                    try {
                        produceChangeMessage(flightTask);
                    } catch (BizException e) {
                        e.printStackTrace();
                    }
                }
            });

            pool.execute(new Runnable() {
                @Override
                public void run() {
            logger.info("<reportcollectionapi><DataCollectionServiceImpl><produceAndSendMessage><produceAndSendMessage><produceAndSendMessage>" + "【任务启动】启动任务，入参=" + JSON.toString(flightTask)

                    + ",结果=生产退票信息" );
                    try {
                        produceRefundMessage(flightTask);
                    } catch (BizException e) {
                        e.printStackTrace();
                    }
                }
            });


            pool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        logger.info("<reportcollectionapi><DataCollectionServiceImpl><assignTask><assignTask><assignTask>" + "【分发报表统计任务】分发报表统计任务，入参=" + JSON.toString(flightTask)
                                + ",结果=分发报表统计任务" );
                        assignTask(flightTask);
                    } catch (BizException e) {
                        e.printStackTrace();
                    }
                }
            });
            pool.shutdown();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void produceAndSendMessage(FlightTask flightTask) throws BizException {
        TaskNodeDO taskNodeDO = new TaskNodeDO();
        taskNodeDO.setDescription("生产发送消息");
        taskNodeDO.setCreatetime(new Date());
        taskNodeDO.setCount(1);
        taskNodeDO.setNodeName("produceAndSendMessage");
        try {
            List<Long> times =  hiveDAO.getCountTime("FLIGHT_ORDER","OUT_TICKET_TIME",new String[]{flightTask.getContext().getStartTime(),flightTask.getContext().getEndTime()});
            RedisUtils.set("createNumFlag","0",Constant.VALID_TIME);
            if(times!=null && times.size()>0){
                RedisUtils.set("createNum",times.get(0)+"",Constant.VALID_TIME);
                logger.info("<reportcollectionapi><DataCollectionServiceImpl><produceAndSendMessage><produceAndSendMessage><createNum>" + "【任务启动】启动任务，入参=" +  JSON.toString(flightTask)
                        + ",结果=cunrudezhi++++++createNum++++++++++++++++++++++++++++++++" + times.get(0));
                System.out.println("cunrudezhi++++++createNum++++++++++++++++++++++++++++++++"+times.get(0));
                if(times.get(1) !=0){
                    for (int i = 1; i <= times.get(1); i++) {
                        List<Map<String, Object>>  result = hiveDAO.getResultByPage(i,new String[]{flightTask.getContext().getStartTime(),flightTask.getContext().getEndTime()});
                        for(Map<String, Object> map:result){
                            TopicAndTag topicAndTag = new TopicAndTag();
                            topicAndTag.setTopic(Constant.CREATE_TOPIC);
                            topicAndTag.setTag(Constant.CREATE_TOPIC);
                            map.put("context",flightTask.getContext());
                            topicAndTag.setObject(map);
                            topicAndTag.setGroupName(Constant.MESSAGE_GROUP);
                            messageProduct.process(topicAndTag);
                        }
                    }
                    taskNodeDO.setNodeState(TaskNodeEnum.SUCCESS.getCode());
                }
            }
        }catch (Exception e){
            logger.error("<reportcollectionapi><DataCollectionServiceImpl><produceAndSendMessage><produceAndSendMessage><createNum>" + "【生产者生产数据】生产者生产数据，入参=" +JSON.toString(flightTask)
                    + ",生产信息异常............" +org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
            taskNodeDO.setNodeState(TaskNodeEnum.FAIL.getCode());
            e.printStackTrace();
            throw  new BizException(BizErrorFactory.getInstance().sendAndProductError("生产发送信息失败"));
        }
    }

    @Override
    public void produceChangeMessage(FlightTask flightTask) throws BizException {

        TaskNodeDO taskNodeDO = new TaskNodeDO();
        taskNodeDO.setDescription("生产改签发送消息");
        taskNodeDO.setCreatetime(new Date());
        taskNodeDO.setCount(1);
        taskNodeDO.setNodeName("produceChangeMessage");
        try {
            List<Long> times =    hiveDAO.getCountTime("FLIGHT_CHANGE_APPLY","CHANGE_FINISH_DATE",new String[]{flightTask.getContext().getStartTime(),flightTask.getContext().getEndTime()});
            RedisUtils.set("changeNumFlag","0",Constant.VALID_TIME);
            if(times!=null && times.size()>0){
                RedisUtils.set("changeNum",times.get(0)+"",Constant.VALID_TIME);
                logger.info("<reportcollectionapi><DataCollectionServiceImpl><produceChangeMessage><produceChangeMessage><changeNum>" + "【获取生产改签信息总数】获取生产改签信息总数，入参=" +  JSON.toString(flightTask)
                        + ",结果=cunrudezhi++++++changeNum++++++++++++++++++++++++++++++++" + times.get(0));
                System.out.println("cunrudezhi++++++changeNum++++++++++++++++++++++++++++++++"+times.get(0));
                if(times.get(1) !=0){
                    for (int i = 1; i <= times.get(1); i++) {
                        List<Map<String, Object>>  result = hiveDAO.getChangeResultByPage(i,new String[]{flightTask.getContext().getStartTime(),flightTask.getContext().getEndTime()});
                        for(Map<String, Object> map:result){
                            TopicAndTag topicAndTag = new TopicAndTag();
                            topicAndTag.setTopic(Constant.CHANGE_TOPIC);
                            topicAndTag.setTag(Constant.CHANGE_TOPIC);
                            map.put("context",flightTask.getContext());
                            topicAndTag.setObject(map);
                            topicAndTag.setGroupName(Constant.MESSAGE_GROUP);
                            messageProduct.process(topicAndTag);
                        }
                    }
                    taskNodeDO.setNodeState(TaskNodeEnum.SUCCESS.getCode());
                }
            }
        }catch (Exception e){
            logger.error("<reportcollectionapi><DataCollectionServiceImpl><produceChangeMessage><produceChangeMessage><changeNum>" + "【生产改签数据错误】生产改签数据错误，入参=" +JSON.toString(flightTask)
                    + ",生产改签信息异常............" +org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
            taskNodeDO.setNodeState(TaskNodeEnum.FAIL.getCode());
            e.printStackTrace();
            taskNodeDO.setNodeState(TaskNodeEnum.FAIL.getCode());
//            throw  new BizException(BizErrorFactory.getInstance().sendAndProductError("生产发送改签信息失败"));
        }



    }

    @Override
    public void produceRefundMessage(FlightTask flightTask) throws BizException {

        TaskNodeDO taskNodeDO = new TaskNodeDO();
        taskNodeDO.setDescription("生产退票发送消息");
        taskNodeDO.setCreatetime(new Date());
        taskNodeDO.setCount(1);
        taskNodeDO.setNodeName("produceRefundMessage");
        try {
            List<Long> times =  hiveDAO.getCountTime("FLIGHT_REFUND_APPLY","REFUND_FINISH_DATE",new String[]{flightTask.getContext().getStartTime(),flightTask.getContext().getEndTime()});
            RedisUtils.set("refundNumFlag","0",Constant.VALID_TIME);
            if(times!=null && times.size()>0){
//                zookeeperExecutor.createPath("/" + Constant.ZK_NAMESPACE + "/refundNum" , ZKNodeStatus.REGISTER.getCode().getBytes());
                RedisUtils.set("refundNum",times.get(0)+"",Constant.VALID_TIME);
                logger.info("<reportcollectionapi><DataCollectionServiceImpl><produceRefundMessage><produceRefundMessage><refundNum>" + "【获取退票信息总数】获取退票信息总数，入参=" +  JSON.toString(flightTask)
                        + ",结果=cunrudezhi++++++refundNum++++++++++++++++++++++++++++++++" + times.get(0));
                System.out.println("cunrudezhi++++++refundNum++++++++++++++++++++++++++++++++"+times.get(0));

                if(times.get(1) !=0){
                    List<TopicAndTag> topicAndTagList = new ArrayList<>();
                    for (int i = 1; i <= times.get(1); i++) {
                        List<Map<String, Object>>  result = hiveDAO.getRefundResultByPage(i,new String[]{flightTask.getContext().getStartTime(),flightTask.getContext().getEndTime()});
                        for(Map<String, Object> map:result){
                            TopicAndTag topicAndTag = new TopicAndTag();
                            topicAndTag.setTopic(Constant.REFUND_TOPIC);
                            topicAndTag.setTag(Constant.REFUND_TOPIC);
                            map.put("context",flightTask.getContext());
                            topicAndTag.setObject(map);
                            topicAndTag.setGroupName(Constant.MESSAGE_GROUP);
                            topicAndTagList.add(topicAndTag);
                            messageProduct.process(topicAndTag);
                        }
                    }
                    taskNodeDO.setNodeState(TaskNodeEnum.SUCCESS.getCode());
                }
            }
        }catch (Exception e){
            logger.error("<reportcollectionapi><DataCollectionServiceImpl><produceRefundMessage><produceRefundMessage><refundNum>" + "【生产退票数据错误】生产退票数据错误，入参=" +JSON.toString(flightTask)
                    + ",生产退票信息异常............" +org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
            taskNodeDO.setNodeState(TaskNodeEnum.FAIL.getCode());
            e.printStackTrace();
            taskNodeDO.setNodeState(TaskNodeEnum.FAIL.getCode());
//            throw  new BizException(BizErrorFactory.getInstance().sendAndProductError("生产发送退票信息失败"));
        }
    }



    public void  assignTask(FlightTask flightTask)throws  BizException{
        try {
            for (ReportAnalysisEnum reportAnalysisEnum : ReportAnalysisEnum.values()) {
                TopicAndTag topicAndTag = new TopicAndTag();
                topicAndTag.setTopic(Constant.ASSIGN_TOPIC);
                topicAndTag.setTag(Constant.ASSIGN_TOPIC);
                TaskMessageBody taskMessageBody = new TaskMessageBody();
                taskMessageBody.setName(reportAnalysisEnum.getKey());
                taskMessageBody.setCode(reportAnalysisEnum.getValue());
                taskMessageBody.setFlightTask(flightTask);
                topicAndTag.setObject(taskMessageBody);
                topicAndTag.setGroupName(Constant.MESSAGE_GROUP);
                messageProduct.process(topicAndTag);
            }

        }catch (Exception e){
            logger.error("<reportcollectionapi><DataCollectionServiceImpl><assignTask><assignTask><assignTask>" + "【生产者生产数据】生产者生产数据，入参=" +JSON.toString(flightTask)
                    + ",生产信息异常............" +org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
        }
    }
}
