package com.ly.report.tmc.datacollectionapi.biz.task;

/**
 * Created by hyz46086 on 2017/4/21.
 */
public enum  TaskNodeEnum {

    SUCCESS(0,"处理成功") {
        @Override
        public int getCode() {
            return 0;
        }
    },


    FAIL(1,"处理失败") {
        @Override
        public int getCode() {
            return 1;
        }
    },

    START(2,"开始") {
        @Override
        public int getCode() {
            return 2;
        }
    };

    /** code */
    private int code;

    /** description */
    private String desc;


    TaskNodeEnum(int code, String desc) {
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
