package com.ly.report.tmc.datacollectionapi.web.controller;

import com.ly.report.tmc.datacollectionapi.biz.collection.DataCollectionService;


import com.ly.report.tmc.datacollectionapi.biz.collection.DataCollectionUpdateService;
import com.ly.report.tmc.datacollectionapi.biz.collection.impl.DataCollectionServiceImpl;
import com.ly.report.tmc.datacollectionapi.web.controller.error.WebErrorFactory;
import com.ly.report.tmc.datacollectionapi.web.controller.exception.WebStatisticsException;
import com.ly.report.tmc.datacollectionapi.web.controller.util.DateUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;

/**
 * 报表统计任务启动接口
 * Created by hyz46086 on 2017/4/14.
 */

@Controller
public class DayJobController {


    @Resource
    DataCollectionService dataCollectionService;
    @Resource
    DataCollectionUpdateService dataCollectionUpdateService;

    /** logger */
    private Logger logger = LoggerFactory.getLogger(DayJobController.class);


    @RequestMapping(value = "/startJob" ,method = RequestMethod.POST)
    @ResponseBody
    public ResultMsg startStatistics(  @RequestParam(required = false) String startTime,
                                  @RequestParam(required = false) String endTime) throws WebStatisticsException{

        try {

            if(StringUtils.isBlank(startTime)){
                String nowDate = DateUtil.getYesterdayTime();
                startTime= nowDate+" 00:00:00";
                endTime = nowDate+" 23:59:59";
            }

            if(!(StringUtils.isBlank(startTime)) && !( StringUtils.isBlank(endTime))){
                startTime= startTime+" 00:00:00";
                endTime = endTime+" 23:59:59";
            }

            if(!(StringUtils.isBlank(startTime)) &&  StringUtils.isBlank(endTime)){
                startTime= startTime+" 00:00:00";
                endTime = startTime+" 23:59:59";
            }

            dataCollectionUpdateService.startJob(startTime,endTime);
        }catch (Exception e){
            logger.error("<reportcollectionapi><DayJobController><startStatistics><startStatistics><startStatistics>" + "【任务启动】启动任务，入参=" + startTime+endTime
                    + ",结果=" + e.getStackTrace().toString());
            throw new WebStatisticsException(WebErrorFactory.getInstance().StartError("启动失败"));
        }
        return  new ResultMsg();
    }

}
