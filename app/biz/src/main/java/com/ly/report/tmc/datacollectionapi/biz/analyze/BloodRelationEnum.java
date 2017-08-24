package com.ly.report.tmc.datacollectionapi.biz.analyze;

/**
 * Created by hyz46086 on 2017/4/25.
 */
public enum BloodRelationEnum {

    DIRECT(0,"直属") {
        @Override
        public int getCode() {
            return 0;
        }
    },

    ATAVISM(1,"隔代") {
        @Override
        public int getCode() {
            return 1;
        }
    };

    /** code */
    private int code;

    /** description */
    private String desc;


    BloodRelationEnum(int code, String desc) {
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
