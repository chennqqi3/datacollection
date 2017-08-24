/**
 * LY.com Inc.
 * Copyright (c) 2004-2017 All Rights Reserved.
 */
package com.ly.report.tmc.datacollectionapi.biz.zk;

import com.ly.report.tmc.datacollectionapi.biz.zk.impl.PreliminaryNodeChildrenCallback;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author hyz46086
 * @version $Id: ZKPathChildrenListen, v 0.1 2017/6/26 11:13 hyz46086 Exp $
 */
public class ZKPathChildrenListen  implements PathChildrenCacheListener {

    private NodeChildrenCallback nodeChildrenCallback;

    private PathChildrenCache childrenCache;

    private PathChildrenCacheEvent.Type[] _types;

    private static volatile ZKPathChildrenListen zkl;

    public ZKPathChildrenListen(){};

    public ZKPathChildrenListen(NodeChildrenCallback nodeChildrenCallback,PathChildrenCache childrenCache,PathChildrenCacheEvent.Type... types){
        this.nodeChildrenCallback=nodeChildrenCallback;
        this.childrenCache=childrenCache;
        this._types=types;
    };
    public  static int i,j=0;

    public static ZKPathChildrenListen getInstance(NodeChildrenCallback nodeChildrenCallback,PathChildrenCache childrenCache,PathChildrenCacheEvent.Type... types){
        if (zkl == null) {
            synchronized (ZKPathChildrenListen.class) {
                if (zkl == null) {
                    zkl = new ZKPathChildrenListen(nodeChildrenCallback,childrenCache,types);
                }
            }
        }
        return zkl;
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {

    }
}
