package com.ly.report.tmc.datacollectionapi.biz.analyze;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.ly.report.tmc.datacollectionapi.biz.zk.ZookeeperExecutor;

/**
 * Created by hyz46086 on 2017/5/2.
 */
public interface DataAnalyzeService {

//    public void analyze(ZookeeperExecutor zke,MessageExt messageExt);
    public void analyze(MessageExt messageExt);
}
