/**
 * LY.com Inc.
 * Copyright (c) 2004-2017 All Rights Reserved.
 */
package com.ly.report.tmc.datacollectionapi.dal.hive.db.to;

/**
 * @author hyz46086
 * @version $Id: OrderPassengerTickUpdate, v 0.1 2017/5/26 14:54 hyz46086 Exp $
 */
public class OrderPassengerTickUpdate {

    private String passengerId;

    private String createTime;

    private String codeId;

    private String finishTime;

    private String tmcId;

    private String itemId;

    private String passengerDepId;

    private String passengerSId;

    private String companyId;

    private String orderNum;

    private String passengerType;

    public String getPassengerId() {

        return passengerId;
    }

    public void setPassengerId(String passengerId) {
        this.passengerId = passengerId;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getCodeId() {
        return codeId;
    }

    public void setCodeId(String codeId) {
        this.codeId = codeId;
    }

    public String getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(String finishTime) {
        this.finishTime = finishTime;
    }

    public String getTmcId() {
        return tmcId;
    }

    public void setTmcId(String tmcId) {
        this.tmcId = tmcId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getPassengerDepId() {
        return passengerDepId;
    }

    public void setPassengerDepId(String passengerDepId) {
        this.passengerDepId = passengerDepId;
    }

    public String getPassengerSId() {
        return passengerSId;
    }

    public void setPassengerSId(String passengerSId) {
        this.passengerSId = passengerSId;
    }

    public String getCompanyId() {
        return companyId;
    }

    public void setCompanyId(String companyId) {
        this.companyId = companyId;
    }

    public String getOrderNum() {
        return orderNum;
    }

    public void setOrderNum(String orderNum) {
        this.orderNum = orderNum;
    }

    public String getPassengerType() {
        return passengerType;
    }

    public void setPassengerType(String passengerType) {
        this.passengerType = passengerType;
    }
}
