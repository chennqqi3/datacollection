package com.ly.report.tmc.datacollectionapi.biz.analyze;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.rocketmq.common.message.MessageExt;

import com.ly.flight.tmc.reportanalysisapi.facade.dto.RequestDTO;
import com.ly.report.tmc.datacollectionapi.biz.common.Constant;
import com.ly.report.tmc.datacollectionapi.biz.consumer.MessageConsumer;
import com.ly.report.tmc.datacollectionapi.biz.listener.MessageListener;
import com.ly.report.tmc.datacollectionapi.biz.redis.RedisUtils;
import com.ly.report.tmc.datacollectionapi.biz.task.Context;
import com.ly.report.tmc.datacollectionapi.biz.util.StringHelper;
import com.ly.report.tmc.datacollectionapi.biz.util.UUIDBuild;
import com.ly.report.tmc.datacollectionapi.biz.zk.ZKNodeStatus;
import com.ly.report.tmc.datacollectionapi.biz.zk.ZookeeperExecutor;
import com.ly.report.tmc.datacollectionapi.dal.common.DalConstant;
import com.ly.report.tmc.datacollectionapi.dal.hive.db.ChangeHiveDAO;
import com.ly.report.tmc.datacollectionapi.dal.hive.db.HiveDAO;
import com.ly.report.tmc.datacollectionapi.dal.hive.db.RefundHiveDAO;
import com.ly.report.tmc.datacollectionapi.dal.hive.db.to.OrderPassengerTick;
import com.ly.report.tmc.datacollectionapi.dal.message.TopicContext;
import com.ly.report.tmc.datacollectionapi.integration.ReportAnalysisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by hyz46086 on 2017/4/17.
 */

@Service
public class DataAnalyze  implements DataAnalyzeService {

    @Resource
    private  HiveDAO hiveDAO;

    @Resource
    private ChangeHiveDAO changeHiveDAO;

    @Resource
    private RefundHiveDAO refundHiveDAO;

    @Resource
    private ReportAnalysisClient reportAnalysisClient;

    /** logger */
    private Logger logger = LoggerFactory.getLogger(DataAnalyze.class);
    List<String> taskList = new ArrayList<>();
//    List<String> taskList = new CopyOnWriteArrayList<>();

    @Override
    public void analyze(MessageExt messageExt){

        logger.info("<reportcollectionapi><DataAnalyze><analyze><analyzeStart><"+ messageExt.getMsgId()+">【analyze数据分析】数据分析开始执行，入参=" + messageExt.toString()
                + ",当前时间**********"+new Date().getTime()+"**********************"+Thread.currentThread().getName() );
        String topic = messageExt.getTopic();
        String message = new String(messageExt.getBody());
            JSONObject temp =  JSONObject.parseObject(message);
            String codeId = temp.getString("codeid");

            RequestDTO requestDTO = new RequestDTO();
            if(temp.getJSONObject("context")!=null){
                requestDTO.setEndDate(temp.getJSONObject("context").getString("endTime"));
                requestDTO.setStartDate(temp.getJSONObject("context").getString("startTime"));
            }
            if(Constant.ASSIGN_TOPIC.equals(topic)){
               String taskName =  JSONObject.parseObject(message).getString("name");
               JSONObject flightTask = JSONObject.parseObject(message).getJSONObject("flightTask");
               if(flightTask!=null){
                   JSONObject context = flightTask.getJSONObject("context");
                   String startTime =  context.getString("startTime");
                   String endTime =  context.getString("endTime");
                   requestDTO.setStartDate(startTime);
                   requestDTO.setEndDate(endTime);
               }
                taskList.add(taskName);
            }
            if(Constant.CREATE_TOPIC.equals(topic)){
                //获取该订单的Item信息
                List<OrderPassengerTick> orderPassengerTickListCreate =null;
                List<Object[]> argsList = new ArrayList<>();
                try{
                    logger.info("<reportcollectionapi><DataAnalyze><analyze><assembleCreateTopic><"+ messageExt.getMsgId()+">【查询数据并组装】查询数据并组装，入参=" + messageExt.toString()
                            + "查询数据并组装开始时间..........." +new Date().getTime());
                    orderPassengerTickListCreate = hiveDAO.getOrderItemByOrderNum(codeId);
                    if(orderPassengerTickListCreate!=null && orderPassengerTickListCreate.size()>0){
                        AssemplyData(messageExt,topic, temp, argsList,orderPassengerTickListCreate,OrderStatusEnum.PRIMITIVE.getCode());
                    }
                    logger.info("<reportcollectionapi><DataAnalyze><analyze><assembleCreateTopic><"+ messageExt.getMsgId()+">【查询数据并组装结束】查询数据并组装结束，入参=" + messageExt.toString()
                            + "查询数据并组装结束时间..........." +new Date().getTime());
                    if(argsList!=null && argsList.size()>0){
                        logger.info("<reportcollectionapi><DataAnalyze><createTopic><insertCreateTopic><"+ messageExt.getMsgId()+">【argsList开始插入数据参数】argsList 插入数据参数，入参=" + messageExt.toString()
                                + ",argsList 插入数据参数............" +JSONObject.toJSONString(argsList)+"开始插入时间：：：：：："+new Date().getTime());
                            insertPartition(messageExt, temp.getJSONObject("context").getString("startTime").split(" ")[0], argsList);
                        logger.info("<reportcollectionapi><DataAnalyze><createTopic><insertCreateTopic><"+ messageExt.getMsgId()+">【argsList 结束插入数据参数】argsList 插入数据参数，入参=" + messageExt.toString()
                                + ",argsList 插入数据参数............" +JSONObject.toJSONString(argsList)+"结束插入时间：：：：：："+new Date().getTime());
                    }
                    RedisUtils.incrKey("createNumFlag",1);
                    logger.info("<reportcollectionapi><DataAnalyze><analyze><createNumFlag><"+ messageExt.getMsgId()+">【topic数据分析】topic数据分析，入参=" + messageExt.toString()
                            + ",结果当前变createNumFlag值------。。。。。。" +RedisUtils.getValue("createNumFlag")+"doInvoke开始调用时间：：：："+new Date().getTime());
                    doInvoke(requestDTO);
                    logger.info("<reportcollectionapi><DataAnalyze><analyze><overTopic><"+ messageExt.getMsgId()+">【topic数据分析】topic数据分析，入参=" + messageExt.toString()
                            +"doInvoke调用结束时间：：：："+new Date().getTime());
                }catch (Exception e){
                    logger.error("<reportcollectionapi><DataAnalyze><analyze><analyze><createTopic>" + "【topic数据分析】topic数据分析，入参=" + messageExt.toString()
                            + ",异常订单号..........."+JSONObject.toJSONString(argsList)+".............." +org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
                    e.printStackTrace();
                }
            }
            if(Constant.CHANGE_TOPIC.equals(topic)){
                List<OrderPassengerTick> orderPassengerTickListChange =null;
                List<Object[]> changeArgsList = new ArrayList<>();
                logger.info("<reportcollectionapi><DataAnalyze><analyze><analyze><changeTopic>" + "【change_topic数据分析】change_topic数据分析，入参=" + messageExt.toString()
                    + ",结果改签订单号............" +codeId);
                try{

                    orderPassengerTickListChange = changeHiveDAO.getChangeOrderDepByChangeNo(codeId);
                    if(orderPassengerTickListChange!=null && orderPassengerTickListChange.size()>0){
                        AssemplyData(messageExt,topic, temp, changeArgsList,orderPassengerTickListChange,OrderStatusEnum.CHANGE.getCode());
                    }
                    if(changeArgsList!=null && changeArgsList.size()>0){
                        logger.info("<reportcollectionapi><DataAnalyze><analyze><changeTopic><changeArgsList>" + "【changeArgsList 插入数据参数】changeArgsList 插入数据参数，入参=" + messageExt.toString()
                                + ",changeArgsList 插入数据参数............" +JSONObject.toJSONString(changeArgsList));
                        insertPartition(messageExt, temp.getJSONObject("context").getString("startTime").split(" ")[0],changeArgsList);
//                        insertPartition(messageExt, temp.getJSONObject("context").getString("startTime").split(" ")[0], changeArgsList,OrderStatusEnum.CHANGE.getCode());
                    }
                    RedisUtils.incrKey("changeNumFlag",1);
                    logger.info("<reportcollectionapi><DataAnalyze><analyze><changeTopic><changeNumFlag>" + "【changeNumFlag】changeNumFlag，入参=" + messageExt.toString()
                            + ",结果当前变changeNumFlag值------。。。。。。" +RedisUtils.getValue("changeNumFlag"));
                    System.out.println("当前变changeNumFlag值------。。。。。。" +RedisUtils.getValue("changeNumFlag"));
                    doInvoke(requestDTO);
                }catch (Exception e){
                    logger.error("<reportcollectionapi><DataAnalyze><analyze><analyze><changeTopic>" + "【change_topic数据分析】change_topic数据分析，入参=" + messageExt.toString()
                            + ",异常改签订单号....."+ JSONObject.toJSONString(changeArgsList)+"......." +org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
                    e.printStackTrace();
                }
            }
            if(Constant.REFUND_TOPIC.equals(topic)){
                List<OrderPassengerTick> orderPassengerTickListRefund =null;
                List<Object[]> refundArgsList = new ArrayList<>();
           try{
               logger.info("<reportcollectionapi><DataAnalyze><analyze><analyze><refundTopic>" + "【refund_topic数据分析】refund_topic数据分析，入参=" + messageExt.toString()
                       + ",结果退款订单号............" +codeId);
               orderPassengerTickListRefund = refundHiveDAO.getRefundOrderDepByChangeNo(codeId);
               if(orderPassengerTickListRefund!=null && orderPassengerTickListRefund.size()>0){
                   AssemplyData(messageExt,topic, temp, refundArgsList,orderPassengerTickListRefund,OrderStatusEnum.REFUND.getCode());
               }
               if(refundArgsList!=null && refundArgsList.size()>0){
                   logger.info("<reportcollectionapi><DataAnalyze><analyze><refundTopic><refundArgsList>" + "【refundArgsList 插入数据参数】refundArgsList 插入数据参数，入参=" + messageExt.toString()
                           + ",refundArgsList 插入数据参数............" +JSONObject.toJSONString(refundArgsList));
                   insertPartition(messageExt, temp.getJSONObject("context").getString("startTime").split(" ")[0], refundArgsList);
               }
               RedisUtils.incrKey("refundNumFlag",1);
               logger.info("<reportcollectionapi><DataAnalyze><analyze><refundTopic><refundNumFlag>" + "【refund_topic数据分析】refund_topic数据分析，入参=" + messageExt.toString()
                       + ",结果当前变refundNumFlag值............" +RedisUtils.getValue("refundNumFlag"));
               doInvoke(requestDTO);

           }catch (Exception e){
               logger.error("<reportcollectionapi><DataAnalyze><analyze><analyze><refundTopic>" + "【refund_topic数据分析】refund_topic数据分析，入参=" + messageExt.toString()
                       + ",异常退款订单号...."+ JSONObject.toJSONString(refundArgsList)+"........" +org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
                e.printStackTrace();
           }
        }
    }

    private void insertPartition(MessageExt messageExt, String partitionTime, List<Object[]> argsList) {
//        hiveDAO.dropTable(messageExt.getMsgId());
//        hiveDAO.createTable(messageExt.getMsgId());
        hiveDAO.batchInsertOrderDep(argsList,  "Temporary_"+ StringHelper.deleteLine(partitionTime));
//        hiveDAO.batchInsertOrderDep(argsList,  messageExt.getMsgId());
//        hiveDAO.batchInsertOrderByPartition(new Object[]{"'"+partitionTime+"'", messageExt.getMsgId()});
//        hiveDAO.dropTable(messageExt.getMsgId());
    }

    private void AssemplyData(MessageExt messageExt,String topic, JSONObject temp, List<Object[]> argsList,List<OrderPassengerTick> orderPassengerTicks,int code) {
        for(OrderPassengerTick ot : orderPassengerTicks ){
            //放入当前部门的id 创建订单参数
            ArrayList<Object> objects = new ArrayList<>();
            objects.add(UUIDBuild.getInstance().generate());
            objects.add(ot.getPassengerDepId());
            objects.add(ot.getOrderNum());
            objects.add(ot.getPassengerId());
            objects.add(ot.getPassengerType());
            objects.add(StringHelper.stampToDate(temp.getString("finishtime")));
            objects.add(temp.getString("tmcid"));
            objects.add(ot.getCompanyId());
            objects.add(code);
            objects.add(BloodRelationEnum.ATAVISM.getCode());
            objects.add(ot.getItemId());
            objects.add(StringHelper.stampToDate(temp.getString("createtime")));
            objects.add(topic);
            objects.add(messageExt.getMsgId());
            objects.add(temp.getJSONObject("context").getString("startTime").split(" ")[0]);
            argsList.add(objects.toArray());
            //拆分获取父id 创建订单参数
            if(!("".equals(ot.getPassengerSId())) && ot.getPassengerSId()!=null){
                String[] ids = ot.getPassengerSId().split(",");
                if(ids!=null && ids.length>0){
                    for(int j=0;j<ids.length;j++){
                        if("".equals(ids[j])||ids[j]==null)continue;
                        ArrayList<Object> argsObjects = new ArrayList<>();
                        argsObjects.add(UUIDBuild.getInstance().generate());
                        argsObjects.add(ids[j]);
                        argsObjects.add(ot.getOrderNum());
                        argsObjects.add(ot.getPassengerId());
                        argsObjects.add(ot.getPassengerType());
                        argsObjects.add(StringHelper.stampToDate(temp.getString("finishtime")));
                        argsObjects.add(temp.getString("tmcid"));
                        argsObjects.add(ot.getCompanyId());
                        argsObjects.add(code);
                        objects.add(ot.getItemId());
                        objects.add(StringHelper.stampToDate(temp.getString("createtime")));
                        objects.add(messageExt.getMsgId());
                        objects.add(temp.getJSONObject("context").getString("startTime").split(" ")[0]);
                        argsObjects.add(topic);
                        if( ids[0]!=null || (ids[0]==null && ids[1]!=null)){
                            argsObjects.add(BloodRelationEnum.DIRECT.getCode());
                        }else{
                            argsObjects.add(BloodRelationEnum.ATAVISM.getCode());
                        }
                        argsList.add(argsObjects.toArray());
                    }
                }
            }
        }
    }
    private void  doInvoke(RequestDTO requestDTO ){
       if(RedisUtils.isExistsString("createNum") || RedisUtils.isExistsString("changeNum") || RedisUtils.isExistsString("refundNum") ){
           if(!("0".equals(RedisUtils.getValue("createNum"))) || !("0".equals(RedisUtils.getValue("changeNum"))) || !("0".equals(RedisUtils.getValue("refundNum")))){
               if(RedisUtils.getValue("createNum").equals(RedisUtils.getValue("createNumFlag")) && RedisUtils.getValue("refundNum").equals(RedisUtils.getValue("refundNumFlag")) && RedisUtils.getValue("changeNum").equals(RedisUtils.getValue("changeNumFlag"))){
                   if(taskList!=null && taskList.size()>0){
                       String partitionTime = requestDTO.getStartDate().split(" ")[0];
                       try {
                           Long TotalTem =  hiveDAO.getTotalTime("Temporary_"+ StringHelper.deleteLine(partitionTime));
                           logger.info("<reportcollectionapi><DataAnalyze><doInvoke><doInvoke><startDB>" + "【临时表转移至正式库开始】临时表转移至正式库开始，入参=" + requestDTO.toString()
                                   + ",临时表开始时间............" +new Date().getTime()+"转移的总数据.........."+TotalTem);
                           hiveDAO.batchInsertOrderByPartition(new Object[]{"'"+partitionTime+"'","Temporary_"+ StringHelper.deleteLine(partitionTime)});
                           logger.info("<reportcollectionapi><DataAnalyze><doInvoke><doInvoke><overDB>" + "【临时表转移至正式库结束】临时表转移至正式库结束，入参=" + requestDTO.toString()
                                   + ",临时表结束时间............" +new Date().getTime());
                       }catch (Exception e){
                           logger.error("<reportcollectionapi><DataAnalyze><doInvoke><doInvoke><insertDB>" + "【临时表转移至正式表异常】临时表转移至正式表异常，入参=" + requestDTO.toString()
                                   + ",临时表转移至正式表异常............" +org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
                           e.printStackTrace();
                       }
                logger.info("<reportcollectionapi><DataAnalyze><doInvoke><doInvoke><over>" + "【唤醒任务doInvoke】唤醒任务doInvoke，入参=" + JSON.toJSONString(requestDTO)
                               + ",结果  开始调用接口--------------" );

                   }
                   taskList.clear();
                   hiveDAO.dropTable("Temporary_"+ StringHelper.deleteLine(requestDTO.getStartDate().split(" ")[0]));
                   RedisUtils.set("createNumFlag","0",5000);
                   RedisUtils.set("refundNumFlag","0",5000);
                   RedisUtils.set("changeNumFlag","0",5000);
               }
           }
       }
    }
}
