package com.ly.report.tmc.datacollectionapi.dal.hive.db.impl;


import com.ly.report.tmc.datacollectionapi.dal.common.DalConstant;
import com.ly.report.tmc.datacollectionapi.dal.hive.db.RefundHiveDAO;
import com.ly.report.tmc.datacollectionapi.dal.hive.db.to.OrderPassengerTick;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.annotation.Resource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by hyz46086 on 2017/4/25.
 */
public class RefundHiveDAOImpl implements RefundHiveDAO {
    @Resource
    private JdbcTemplate jdbcTemplate;

    @Override
    public List<OrderPassengerTick> getRefundOrderDepByChangeNo(String refundNo) {
        String sql = "SELECT pa.ID as passengerId,foi.ID as itemId ,pa.PASSENGER_DEPARTMENT_ID as passengerDepId ,pa.PASSENGER_STRUCTURE_ID as passengersId ,pa.PASSENGER_COMPANY_ID as companyId,foi.FLIGHT_ORDER_NO as orderNum ,pa.PASSENGER_CLASS as passengerType from "+ DalConstant.DATABASE+".FLIGHT_REFUND_APPLY_ITEM as frai LEFT JOIN "+DalConstant.DATABASE+".FLIGHT_REFUND_APPLY as fra ON frai.FLIGHT_REFUND_APPLY_NO = fra.APPLY_NO LEFT JOIN "+DalConstant.DATABASE+".FLIGHT_ORDER_ITEM as foi ON frai.ORDER_ITEM_ID=foi.ID LEFT JOIN "+DalConstant.DATABASE+".PASSENGER as pa on foi.PASSENGER_ID=pa.ID where fra.APPLY_NO=? and fra.REFUND_STATUS=?";
//        String sql = "SELECT pa.ID as passengerId, pa.PASSENGER_DEPARTMENT_ID as passengerDepId ,pa.PASSENGER_STRUCTURE_ID as passengersId ,pa.PASSENGER_COMPANY_ID as companyId,foi.FLIGHT_ORDER_NO as orderNum ,pa.PASSENGER_TYPE as passengerType from FLIGHT_REFUND_APPLY_ITEM as frai LEFT JOIN FLIGHT_REFUND_APPLY as fra ON frai.FLIGHT_REFUND_APPLY_NO = fra.APPLY_NO LEFT JOIN FLIGHT_ORDER_ITEM as foi ON frai.ORDER_ITEM_ID=foi.ID LEFT JOIN PASSENGER as pa on foi.PASSENGER_ID=pa.ID where fra.APPLY_NO=?";
//        List<OrderPassengerTick> orderPassengerTicks = jdbcTemplate.queryForList(sql,new String[]{changeNo},OrderPassengerTick.class);
        List<OrderPassengerTick> orderPassengerTicks =jdbcTemplate.query(sql,new String[]{refundNo,"07"}, new RowMapper() {
            @Override
            public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                OrderPassengerTick orderPassengerTick = new OrderPassengerTick();
                orderPassengerTick.setPassengerId(rs.getString("passengerid"));
                orderPassengerTick.setCompanyId(rs.getString("companyid"));
                orderPassengerTick.setItemId(rs.getString("itemid"));
                orderPassengerTick.setOrderNum(rs.getString("ordernum"));
                orderPassengerTick.setPassengerDepId(rs.getString("passengerdepid"));
                orderPassengerTick.setPassengerSId(rs.getString("passengersid"));
                orderPassengerTick.setPassengerType(rs.getString("passengertype"));
                return orderPassengerTick;
            }
        });
        return  orderPassengerTicks;
    }
}
