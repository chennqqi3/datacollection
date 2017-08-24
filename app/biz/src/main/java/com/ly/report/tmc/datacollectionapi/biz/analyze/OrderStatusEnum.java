package com.ly.report.tmc.datacollectionapi.biz.analyze;

/**
 * Created by hyz46086 on 2017/4/25.
 */
public enum OrderStatusEnum {

    PRIMITIVE(0,"出票") {
        @Override
        public int getCode() {
            return 0;
        }
    },

    REFUND(1,"退票") {
        @Override
        public int getCode() {
            return 1;
        }
    },

    CHANGE(2,"改签") {
        @Override
        public int getCode() {
            return 2;
        }
    };


    /** code */
    private int code;

    /** description */
    private String desc;



    OrderStatusEnum(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public abstract int getCode();

    public void setCode(int code) {
        this.code = code;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
