package com.ly.report.tmc.datacollectionapi.biz.zk;

import java.io.Serializable;

/**
 * Created by hyz46086 on 2017/4/18.
 */
public interface NodeCallBack extends Serializable {

    public void call(ZKNode node);
}
