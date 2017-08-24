/**
 * LY.com Inc.
 * Copyright (c) 2004-2017 All Rights Reserved.
 */
package com.ly.report.tmc.datacollectionapi.biz.analyze;

import com.ly.report.tmc.datacollectionapi.biz.zk.ZookeeperExecutor;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author hyz46086
 * @version $Id: ListService, v 0.1 2017/6/14 11:40 hyz46086 Exp $
 */
public class ListService {

   static CopyOnWriteArrayList<String> cpList = new CopyOnWriteArrayList<>();

    public  static CopyOnWriteArrayList getSafeList(){

        if (cpList == null) {
            synchronized (ZookeeperExecutor.class) {
                if (cpList == null) {
                    cpList = new CopyOnWriteArrayList<>();
                }
            }
        }
        return cpList;
    }
}
