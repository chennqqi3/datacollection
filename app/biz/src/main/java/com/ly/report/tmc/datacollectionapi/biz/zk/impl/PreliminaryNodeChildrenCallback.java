package com.ly.report.tmc.datacollectionapi.biz.zk.impl;

import com.alibaba.fastjson.JSON;
import com.ly.flight.tmc.reportanalysisapi.facade.dto.RequestDTO;
import com.ly.report.tmc.datacollectionapi.biz.analyze.ListService;
import com.ly.report.tmc.datacollectionapi.biz.analyze.ReportAnalysisEnum;
import com.ly.report.tmc.datacollectionapi.biz.collection.impl.DataCollectionServiceImpl;
import com.ly.report.tmc.datacollectionapi.biz.common.Constant;
import com.ly.report.tmc.datacollectionapi.biz.listener.MessageListener;
import com.ly.report.tmc.datacollectionapi.biz.task.RequsetDtoFactory;
import com.ly.report.tmc.datacollectionapi.biz.util.Springfactory;
import com.ly.report.tmc.datacollectionapi.biz.zk.NodeChildrenCallback;
import com.ly.report.tmc.datacollectionapi.biz.zk.ZKNode;
import com.ly.report.tmc.datacollectionapi.biz.zk.ZookeeperExecutor;
import com.ly.report.tmc.datacollectionapi.integration.ReportAnalysisClient;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.lang.reflect.InvocationTargetException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *初步的数据过滤
 * Created by hyz46086 on 2017/4/18.
 */
@Service
public class PreliminaryNodeChildrenCallback implements NodeChildrenCallback {

    @Resource
    private ReportAnalysisClient reportAnalysisClient;

    private Logger logger = LoggerFactory.getLogger(PreliminaryNodeChildrenCallback.class);

    public  static int i,j=0;

//    private static volatile PreliminaryNodeChildrenCallback pc;
//
//    public PreliminaryNodeChildrenCallback(){};
//    public  static int i,j=0;
//
//    public static PreliminaryNodeChildrenCallback getInstance(){
//        if (pc == null) {
//            synchronized (PreliminaryNodeChildrenCallback.class) {
//                if (pc == null) {
//                    pc = new PreliminaryNodeChildrenCallback();
//                }
//            }
//        }
//        return pc;
//    }



    private ZookeeperExecutor zk = ZookeeperExecutor.getInstance();
    @Override
    public void call(List<ZKNode> nodes) {
        i++;
        for(ZKNode zkNode : nodes){
            System.out.print(zkNode.getPath());
            logger.info("<reportcollectionapi><PreliminaryNodeChildrenCallback><doListNode><doListNode><getNodeData>" + "【开始遍历节点】开始遍历节点节点个数"+nodes.size()+"，入参=" + zkNode.getPath()+"..."+i+".."+new String(zkNode.getData()));
            if("1".equals(new String(zkNode.getData()))){
                invokeAnalyzeList(RequsetDtoFactory.getRequestDTO(null,null), ListService.getSafeList());
           }
        }
    }


    private void invokeAnalyzeList(RequestDTO requestDTO, CopyOnWriteArrayList<String> taskList) {
        j++;
        logger.info("<reportcollectionapi><DataAnalyze><doInvoke><invokeAnalyzeList><invokeAnalyzeList>" + "【数据开始doInvoke】数据开始doInvoke，入参=" + JSON.toJSONString(requestDTO)
                + ",数据开始doInvoke....."+taskList.size()+"......." + JSON.toJSONString(taskList)+"cpList:::::::"+JSON.toJSONString(taskList)+"...........被调用的次数"+ j);
        if(taskList!=null && taskList.size()>0){
            for(String task : taskList){
                if(ReportAnalysisEnum.PASSENGER.getKey().equals(task)){
                    try {
                        reportAnalysisClient.passengerAnalysis(requestDTO);
                        logger.info("<reportcollectionapi><DataAnalyze><doInvoke><invokeAnalyzeList><startInvoke>" + "【数据开始doInvoke】数据开始doInvoke，入参=" + JSON.toJSONString(requestDTO)
                                + ",数据开始doInvoke....."+taskList.size()+"......." + JSON.toJSONString(taskList)+"cpList:::::::"+JSON.toJSONString(taskList)+"...........被调用的次数"+ j);
                    } catch (Exception e) {
                        logger.error("<reportcollectionapi><DataAnalyze><analyze><invokeAnalyzeList><invokeAnalyzeListError>" + "【调用乘机人分析接口出错】............入参数="+JSON.toJSONString(requestDTO)+"异常="
                                + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));

                    }
                }
                if(ReportAnalysisEnum.FLIGHT_CLASS.getKey().equals(task)){
                    try {
                        reportAnalysisClient.flightClassAnalysis(requestDTO);
                    } catch (Exception e) {
                        logger.error("<reportcollectionapi><DataAnalyze><analyze><invokeAnalyzeList><invokeAnalyzeListError>" + "【调用仓等分析接口出错】..............入参数="+JSON.toJSONString(requestDTO)+"异常="
                                + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));

                    }

                }
                //chenwan
                if(ReportAnalysisEnum.AIR_ROUTE.getKey().equals(task)){
                    try {
                        reportAnalysisClient.airRouteCompute(requestDTO);
                    } catch (Exception e) {
                        logger.error("<reportcollectionapi><DataAnalyze><analyze><invokeAnalyzeList><invokeAnalyzeListError>" + "【调用航线分析接口出错】............入参数="+JSON.toJSONString(requestDTO)+"异常="
                                + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));

                    }

                }
                if(ReportAnalysisEnum.DAYS_AHEAD.getKey().equals(task)){
                    try {
                        reportAnalysisClient.computeDaysAhead(requestDTO);
                    } catch (Exception e) {
                        logger.error("<reportcollectionapi><DataAnalyze><analyze><invokeAnalyzeList><invokeAnalyzeListError>" + "【调用提前天数接口出错】............入参数="+JSON.toJSONString(requestDTO)+"异常="
                                + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));

                    }

                }
                if(ReportAnalysisEnum.DEPARTMENT.getKey().equals(task)){
                    try {
                    reportAnalysisClient.computeDepartment(requestDTO);
                } catch (Exception e) {
                    logger.error("<reportcollectionapi><DataAnalyze><analyze><invokeAnalyzeList><invokeAnalyzeListError>" + "【调用部门分析接口出错】............入参数="+JSON.toJSONString(requestDTO)+"异常="
                            + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
                }

                }
                //huangyu
                if(ReportAnalysisEnum.TRAVEL_POLICY.getKey().equals(task)){
                    try {
                        reportAnalysisClient.travelPolicyAnalysis(requestDTO);
                    } catch (Exception e) {
                        logger.error("<reportcollectionapi><DataAnalyze><analyze><invokeAnalyzeList><invokeAnalyzeListError>" + "【调用travelPolicyAnalysis分析接口出错】............入参数="+JSON.toJSONString(requestDTO)+"异常="
                                + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
                    }

                }
                if(ReportAnalysisEnum.DEPARTURE_TIME.getKey().equals(task)){
                    try {
                        reportAnalysisClient.departureTimeAnalysis(requestDTO);
                    } catch (Exception e) {
                        logger.error("<reportcollectionapi><DataAnalyze><analyze><invokeAnalyzeList><invokeAnalyzeListError>" + "【调用departureTimeAnalysis析接口出错】............入参数="+JSON.toJSONString(requestDTO)+"异常="
                                + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
                    }

                }
                if(ReportAnalysisEnum.DISCOUNT.getKey().equals(task)){
                    try {
                        reportAnalysisClient.discountAnalysis(requestDTO);
                    } catch (Exception e) {
                        logger.error("<reportcollectionapi><DataAnalyze><analyze><invokeAnalyzeList><invokeAnalyzeListError>" + "【调用discountAnalysis分析接口出错】............入参数="+JSON.toJSONString(requestDTO)+"异常="
                                + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
                    }
                }
                //jiagntao
                if(ReportAnalysisEnum.AIR_CODE.getKey().equals(task)){
                    try {
                        reportAnalysisClient.airCodeAnalysis(requestDTO);
                    } catch (Exception e) {
                        logger.error("<reportcollectionapi><DataAnalyze><analyze><invokeAnalyzeList><invokeAnalyzeListError>" + "【调用protocolAirCodeAnalysis分析接口出错】............入参数="+JSON.toJSONString(requestDTO)+"异常="
                                + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
                    }

                }
                if(ReportAnalysisEnum.BOOK_TYPE.getKey().equals(task)){
                    try {
                        reportAnalysisClient.bookTypeAnalysis(requestDTO);
                    } catch (Exception e) {
                        logger.error("<reportcollectionapi><DataAnalyze><analyze><invokeAnalyzeList><invokeAnalyzeListError>" + "【调用预定方式bookTypeAnalysis接口出错】............入参数="+JSON.toJSONString(requestDTO)+"异常="
                                + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
                    }

                }
                if(ReportAnalysisEnum.PROTOCOL_AIR_CODE.getKey().equals(task)){
                    try {
                        reportAnalysisClient.protocolAirCodeAnalysis(requestDTO);
                    } catch (Exception e) {
                        logger.error("<reportcollectionapi><DataAnalyze><analyze><invokeAnalyzeList><invokeAnalyzeListError>" + "【调用协议航空接口出错】............入参数="+JSON.toJSONString(requestDTO)+"异常="
                                + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
                    }

                }
            }
            j=0;
            taskList.clear();
        }
    }
}
