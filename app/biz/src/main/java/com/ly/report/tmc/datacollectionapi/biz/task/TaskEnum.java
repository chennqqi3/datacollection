package com.ly.report.tmc.datacollectionapi.biz.task;

/**
 * Created by hyz46086 on 2017/4/21.
 */
public enum TaskEnum {

    START(0,"启动任务") {
        @Override
        public int getCode() {
            return 0;
        }
    },

    PROCESSING(1,"处理中") {
        @Override
        public int getCode() {
            return 1;
        }
    },

    SUCCESS(2,"处理成功") {
        @Override
        public int getCode() {
            return 2;
        }
    },

    FAIL(3,"处理失败") {
        @Override
        public int getCode() {
            return 3;
        }
    };

    /** code */
    private int code;

    /** description */
    private String desc;



    TaskEnum(int code, String desc) {
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
