package com.ly.report.tmc.datacollectionapi.biz.zk;

import java.io.Serializable;

import org.springframework.stereotype.Service;

/**
 * Created by hyz46086 on 2017/4/18.
 */
@Service
public class ZooKeeperClient implements Serializable {

    private String connectString;

    private String namespace;


    public ZooKeeperClient() {

    }

//    public ZookeeperExecutor build(){
//        return new ZookeeperExecutor(Constant.ZK_ADDRESS,  Constant.ZK_NAMESPACE);
//    }


}
