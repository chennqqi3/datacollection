package com.ly.report.tmc.datacollectionapi.biz.zk;

import java.io.Serializable;
import java.nio.charset.Charset;

/**
 * Created by hyz46086 on 2017/4/18.
 */
public class ZKNode implements Serializable {

    private String path;

    private byte[] data;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public String getStringData(){
        return new String(getData(), Charset.forName("utf-8"));
    }

}
