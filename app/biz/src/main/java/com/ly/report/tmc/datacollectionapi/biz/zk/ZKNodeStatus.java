package com.ly.report.tmc.datacollectionapi.biz.zk;

/**
 * Created by hyz46086 on 2017/4/21.
 */
public enum ZKNodeStatus {

    REGISTER("0","注册状态") {
        @Override
        public String getCode() {
            return "0";
        }
    },

    UPDATE("1","更新状态") {
        @Override
        public String getCode() {
            return "1";
        }
    };

    /** code */
    private String code;

    /** description */
    private String desc;


    ZKNodeStatus(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public abstract String getCode();

    public void setCode(String code) {
        this.code = code;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
