package com.ly.report.tmc.datacollectionapi.biz.zk;

import com.alibaba.fastjson.JSON;
import com.ly.report.tmc.datacollectionapi.biz.analyze.DoInvoke;
import com.ly.report.tmc.datacollectionapi.biz.common.Constant;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hyz46086 on 2017/4/18.
 */
@Service
public class ZookeeperExecutor {

    private CuratorFramework curatorFramework;

    private CuratorWatcher curatorWatcher;

    private static volatile ZookeeperExecutor zk;

    private Logger logger = LoggerFactory.getLogger(ZookeeperExecutor.class);

    /**
     * 初始化zookeeper
     *
     */
    public ZookeeperExecutor() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(Constant.ZK_ADDRESS)
                .retryPolicy(retryPolicy)
                .namespace( Constant.ZK_NAMESPACE)
                .build();
        client.start();
        curatorFramework=client;
    }

    public static ZookeeperExecutor getInstance(){
        if (zk == null) {
            synchronized (ZookeeperExecutor.class) {
                if (zk == null) {
                    zk = new ZookeeperExecutor();
                }
            }
        }
        return zk;
    }


    public class CustomZooKeeperException extends RuntimeException{
        public CustomZooKeeperException(Exception e) {
            super(e);
        }

        public CustomZooKeeperException(String message,Exception e) {
            super(message,e);
        }

        public CustomZooKeeperException(String message) {
            super(message);
        }
    }


    /**
     * 路径的创建
     * @param path
     * @return
     */
    public String createPath(String path){
        return createPath(path,new byte[]{}, CreateMode.PERSISTENT);
    }

    public String createEphSequencePath(String path){
        return createPath(path,new byte[]{},CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    public String createPath(String path,byte[] data){
        try{
            if(curatorFramework.checkExists().forPath(path)==null){
                return curatorFramework.create()
                        .creatingParentContainersIfNeeded()
                        .withMode(CreateMode.EPHEMERAL)
                        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                        .forPath(path,data);
            }else return  null;

        }catch (Exception e) {
            throw new ZookeeperExecutor.CustomZooKeeperException(e);
        }
    }

    public String createPath(String path,byte[] data,CreateMode createMode){
        try{
            return curatorFramework.create()
                    .creatingParentContainersIfNeeded()
                    .withMode(createMode)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath(path,data);
        }catch (Exception e) {
            throw new ZookeeperExecutor.CustomZooKeeperException(e);
        }
    }

    /**
     * 更新节点的值
     * @param path
     * @param data
     */
    public void setPath(String path,String data){
        try{
            if(!exists(path)){
                createPath(path);
            }
            logger.info("<reportcollectionapi><DataAnalyze><setPath><setPath><setPath>" + "【设置标记值】="+getPath(path).toString()+"入参数="+data);

            if(!(getPath(path).toString().equals(data))){
                curatorFramework.setData()
                        .forPath(path,data.getBytes("utf-8"));
            }

        }catch (Exception e) {
            throw new ZookeeperExecutor.CustomZooKeeperException(e);
        }
    }


    /**
     * 判断路径是否存在
     * @param path
     * **/
    public void checkExist(String path)throws Exception{

        if(curatorFramework.checkExists().forPath(path)==null){
            System.out.println("路径不存在!");
        }else{
            System.out.println("路径已经存在!");
        }

    }


    /**
     * 删除节点
     * @param path
     */
    public void deletePath(String path){
        try{
            if(curatorFramework.checkExists().forPath(path)!=null){
                curatorFramework.delete()
                        .forPath(path);
            }

        }catch (Exception e) {
            throw new ZookeeperExecutor.CustomZooKeeperException(e);
        }
    }

    /**
     * 获取节点的值
     * @param path
     * @return
     */
    public byte[] getPath(String path){
        try{
            if(curatorFramework.checkExists().forPath(path)==null){
                return "0".getBytes();
            }else{
                return curatorFramework.getData()
                        .forPath(path);
            }

        }catch (Exception e) {
            throw new ZookeeperExecutor.CustomZooKeeperException(e);
        }
    }

    /**
     *节点监控
     * @param path
     * @param nodeCallback
     * @param executor
     * @return
     */
    public NodeCache watchPath(final String path, final NodeCallBack nodeCallback, ExecutorService executor){

        try{
            final NodeCache nodeCache = new NodeCache(curatorFramework, path, false);
            nodeCache.start(true);
            nodeCache.getListenable().addListener(new NodeCacheListener() {
                @Override
                public void nodeChanged() throws Exception {
                    try{
                        ZKNode node=new ZKNode();
                        node.setPath(path);
                        ChildData childData= nodeCache.getCurrentData();
                        if(childData==null) return ;
                        node.setData(childData.getData());
                        nodeCallback.call(node);
                    }catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, executor);
            return nodeCache;
        }catch (Exception e) {
            throw new ZookeeperExecutor.CustomZooKeeperException(e);
        }
    }


    /**
     * 单节点监控
     * @param path
     * @param nodeCallback
     * @return
     */
    public NodeCache watchPath(final String path,final NodeCallBack nodeCallback){
        ExecutorService pool = Executors.newFixedThreadPool(1);
        return watchPath(path, nodeCallback, pool);
    }


    /**
     * 所有子节点监控
     * @param path
     * @param nodeChildrenCallback
     * @param executor
     * @param types
     * @return
     */

    public PathChildrenCache watchChildrenPath(final String path, final NodeChildrenCallback nodeChildrenCallback, ExecutorService executor, PathChildrenCacheEvent.Type... types){
        try{
            final PathChildrenCacheEvent.Type[] _types;
            if(types.length==0){
                _types=new PathChildrenCacheEvent.Type[]{PathChildrenCacheEvent.Type.CHILD_UPDATED};
            }
            else{
                _types=types;
            }
            final PathChildrenCache childrenCache=
                    new PathChildrenCache(curatorFramework, path,true,false, executor);

            childrenCache.start(PathChildrenCache.StartMode.NORMAL);
            childrenCache.getListenable().addListener(new PathChildrenCacheListener() {

                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    try{
                        boolean done=false;
                        for(PathChildrenCacheEvent.Type type:_types){
                            if(type==event.getType()){
                                done=true;
                                break;
                            }
                        }
                        if(!done) return;
                        List<ChildData> childDatas= childrenCache.getCurrentData();
                        List<ZKNode> nodes=new ArrayList<>();
                        for(ChildData childData:childDatas){
                            ZKNode node=new ZKNode();
                            node.setData(childData.getData());
                            node.setPath(childData.getPath());
                            nodes.add(node);
                        }
                        nodeChildrenCallback.call(nodes);
                    }catch (Exception e) {
                        logger.error("<reportcollectionapi><ZookeeperExecutor><ZookeeperExecutor><ZookeeperExecutor><childEvent>" + "【注册监听异常】注册监听异常"
                                +org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
                    }
                }
            }, executor);
            return childrenCache;
        }catch (Exception e) {
            logger.error("<reportcollectionapi><ZookeeperExecutor><ZookeeperExecutor><ZookeeperExecutor><ZookeeperExecutor>" + "【注册监听异常】注册监听异常"
                    +org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
            throw new CustomZooKeeperException(e);
        }
    }

    public PathChildrenCache watchChildrenPath(final String path,final NodeChildrenCallback nodeChildrenCallback,PathChildrenCacheEvent.Type... types){
        ExecutorService pool = Executors.newFixedThreadPool(1);
        return watchChildrenPath(path, nodeChildrenCallback, pool, types);
    }

    public boolean exists(final String path){
        try{
            return curatorFramework.checkExists()
                    .forPath(path)!=null;
        }catch (Exception e) {
            throw new ZookeeperExecutor.CustomZooKeeperException(e);
        }
    }

}
