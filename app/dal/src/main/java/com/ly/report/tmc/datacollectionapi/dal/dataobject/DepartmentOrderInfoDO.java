package com.ly.report.tmc.datacollectionapi.dal.dataobject;

import java.sql.Timestamp;

/**
 * Created by hyz46086 on 2017/4/24.
 */
public class DepartmentOrderInfoDO {

    private String id;

    private String departmentID;

    private String orderID;

    private String passengerID;

    private  int  passengerType;

    private Timestamp createTime;

    private  String companyID;

    private int  tmcId;

    private  int  bloodRelation;

    private  int  orderStatus;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDepartmentID() {
        return departmentID;
    }

    public void setDepartmentID(String departmentID) {
        this.departmentID = departmentID;
    }

    public String getOrderID() {
        return orderID;
    }

    public void setOrderID(String orderID) {
        this.orderID = orderID;
    }

    public String getPassengerID() {
        return passengerID;
    }

    public void setPassengerID(String passengerID) {
        this.passengerID = passengerID;
    }

    public int getPassengerType() {
        return passengerType;
    }

    public void setPassengerType(int passengerType) {
        this.passengerType = passengerType;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }

    public String getCompanyID() {
        return companyID;
    }

    public void setCompanyID(String companyID) {
        this.companyID = companyID;
    }

    public int getTmcId() {
        return tmcId;
    }

    public void setTmcId(int tmcId) {
        this.tmcId = tmcId;
    }

    public int getBloodRelation() {
        return bloodRelation;
    }

    public void setBloodRelation(int bloodRelation) {
        this.bloodRelation = bloodRelation;
    }

    public int getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(int orderStatus) {
        this.orderStatus = orderStatus;
    }
}
