/**
 * LY.com Inc.
 * Copyright (c) 2004-2017 All Rights Reserved.
 */
package com.ly.report.tmc.datacollectionapi.biz.analyze;

import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * @author hyz46086
 * @version $Id: DataAnalyzeUpdateService, v 0.1 2017/5/26 11:35 hyz46086 Exp $
 */
public interface DataAnalyzeUpdateService {
    public void analyze(MessageExt messageExt);
}
