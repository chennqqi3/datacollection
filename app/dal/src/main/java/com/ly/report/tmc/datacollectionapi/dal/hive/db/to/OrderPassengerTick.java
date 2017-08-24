package com.ly.report.tmc.datacollectionapi.dal.hive.db.to;

/**
 * Created by hyz46086 on 2017/4/25.
 */
public class OrderPassengerTick {

    private String passengerId;

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

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }
}
