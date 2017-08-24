/**
 * LY.com Inc.
 * Copyright (c) 2004-2017 All Rights Reserved.
 */
package com.ly.report.tmc.datacollectionapi.biz.analyze;

import com.alibaba.fastjson.JSON;
import com.ly.flight.tmc.reportanalysisapi.facade.dto.RequestDTO;
import com.ly.report.tmc.datacollectionapi.biz.redis.RedisUtils;
import com.ly.report.tmc.datacollectionapi.biz.util.StringHelper;
import com.ly.report.tmc.datacollectionapi.biz.zk.ZookeeperExecutor;
import com.ly.report.tmc.datacollectionapi.dal.hive.db.HiveDAO;
import com.ly.report.tmc.datacollectionapi.integration.ReportAnalysisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.lang.reflect.InvocationTargetException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author hyz46086
 * @version $Id: DoInvoke, v 0.1 2017/5/26 15:29 hyz46086 Exp $
 */
@Service
public class DoInvoke {

    @Resource
    private HiveDAO hiveDAO;


    /**
     * logger
     */
    private Logger logger = LoggerFactory.getLogger(DoInvoke.class);

    boolean flag =false;

    public void doInvoke(RequestDTO requestDTO,CopyOnWriteArrayList<String> taskList) {


        try {
                if (!("0".equals(RedisUtils.getValue("primitiveTotalBatch"))) || !("0".equals(RedisUtils.getValue("changeTotalBatch"))) || !("0".equals(RedisUtils.getValue("refundTotalBatch")))) {
                    if (RedisUtils.getValue("primitiveTotalBatch").equals(RedisUtils.getValue("primitiveFlag")) && RedisUtils.getValue("changeTotalBatch").equals(RedisUtils.getValue("changeFlag")) && RedisUtils.getValue("refundTotalBatch").equals(RedisUtils.getValue("refundFlag"))) {
                        String partitionTime = requestDTO.getStartDate().split(" ")[0];
                        try {
                            Long TotalTem =  hiveDAO.getTotalTime("Temporary_"+ StringHelper.deleteLine(partitionTime));
                            logger.info("<reportcollectionapi><DataAnalyze><doInvoke><doInvoke><startDB>" + "【临时表转移至正式库开始】临时表转移至正式库开始，入参=" + requestDTO.toString()
                                    + ",临时表开始时间............" +new Date().getTime()+"转移的总数据.........."+TotalTem);
                                hiveDAO.batchInsertOrderByPartition(new Object[]{"'"+partitionTime+"'","Temporary_"+ StringHelper.deleteLine(partitionTime)});
                                logger.info("<reportcollectionapi><DataAnalyze><doInvoke><doInvoke><overDB>" + "【临时表转移至正式库结束】临时表转移至正式库结束，入参=" + requestDTO.toString()
                                        + ",临时表结束时间............" +new Date().getTime());
                                hiveDAO.dropTable("Temporary_"+ StringHelper.deleteLine(partitionTime));
                                System.out.println("........start doInvoke...............");
                                ZookeeperExecutor.getInstance().setPath("/TCTMCstatisticsNode/OverFlag","1");
                        }catch (Exception e){
                            requestDTO=null;
                            hiveDAO.dropTable("Temporary_"+ StringHelper.deleteLine(partitionTime));
                            logger.error("<reportcollectionapi><DataAnalyze><doInvoke><doInvoke><insertDB>" + "【临时表转移至正式表异常】临时表转移至正式表异常，入参=" + requestDTO.toString()
                                    + ",临时表转移至正式表异常............" +org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
                            e.printStackTrace();
                        }
                    }
                }
        }catch (Exception e){
            requestDTO=null;
            logger.error("<reportcollectionapi><DataAnalyze><doInvoke><doInvoke><invokeErro>" + "【临时表转移至正式表异常】临时表转移至正式表异常，入参=" + requestDTO.toString()
                    + ",临时表转移至正式表异常............" +org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
            e.printStackTrace();
        }
    }

}
