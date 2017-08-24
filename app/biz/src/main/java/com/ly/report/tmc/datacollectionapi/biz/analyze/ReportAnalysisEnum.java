/**
 * LY.com Inc.
 * Copyright (c) 2004-2017 All Rights Reserved.
 */
package com.ly.report.tmc.datacollectionapi.biz.analyze;

public enum ReportAnalysisEnum {
    /** 乘机人分析 */
    PASSENGER("passenger", "1"),
    /** 航等分析 */
    FLIGHT_CLASS("flightClass", "2"),
    /** 航线 */
    AIR_ROUTE("airRoute", "3"),
    /** 提前天数 */
    DAYS_AHEAD("daysAhead", "4"),
    /** 部门分析 */
    DEPARTMENT("department", "5"),
    /** 差旅政策分析 */
    TRAVEL_POLICY("travelPolicy", "6"),
    /** 时间段分析 */
    DEPARTURE_TIME("departureTime", "7"),
    /** 折扣分析 */
    DISCOUNT("discount", "8"),
    /** 航空公司 */
    AIR_CODE("airCode", "9"),
    /** 预订方式 */
    BOOK_TYPE("bookType", "10"),
    /** 协议航空 */
    PROTOCOL_AIR_CODE("protocolAirCode", "11"),;
    
    private String key ;
    private String value;
    
    private ReportAnalysisEnum(String key, String value) {
        this.key = key;
        this.value = value;
    }
    
    public static ReportAnalysisEnum getEnum(String value){
        
        for(ReportAnalysisEnum daysAheadEnum : ReportAnalysisEnum.values()){
            if(daysAheadEnum.getValue().equals(value)){
                return daysAheadEnum;
            }
        }
        
        return null;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
    
    
}
