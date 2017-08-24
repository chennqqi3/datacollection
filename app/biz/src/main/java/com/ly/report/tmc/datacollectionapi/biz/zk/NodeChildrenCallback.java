package com.ly.report.tmc.datacollectionapi.biz.zk;

import java.util.List;

/**
 * Created by hyz46086 on 2017/4/18.
 */
public interface NodeChildrenCallback {

    public void call(List<ZKNode> nodes);
}
