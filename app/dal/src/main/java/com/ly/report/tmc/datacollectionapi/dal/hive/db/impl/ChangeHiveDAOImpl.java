package com.ly.report.tmc.datacollectionapi.dal.hive.db.impl;


import com.ly.report.tmc.datacollectionapi.dal.common.DalConstant;
import com.ly.report.tmc.datacollectionapi.dal.hive.db.ChangeHiveDAO;
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
public class ChangeHiveDAOImpl implements ChangeHiveDAO {

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Override
    public List<OrderPassengerTick> getChangeOrderDepByChangeNo(String changeNo) {
       String sql = "select  pa.ID as passengerId,pa.PASSENGER_DEPARTMENT_ID as passengerDepId,foi.ID as itemId,pa.PASSENGER_STRUCTURE_ID as passengerSId,pa.PASSENGER_COMPANY_ID as companyId,foi.FLIGHT_ORDER_NO as orderNum,pa.PASSENGER_CLASS as passengerType from "+ DalConstant.DATABASE+".FLIGHT_CHANGE_APPLY_ITEM as fcai LEFT JOIN "+DalConstant.DATABASE+".FLIGHT_CHANGE_APPLY as fca ON fcai.APPLY_NO=fca.APPLY_NO LEFT JOIN "+DalConstant.DATABASE+".FLIGHT_ORDER_ITEM as foi ON fcai.NEW_ORDER_ITEM_ID = foi.ID LEFT JOIN "+DalConstant.DATABASE+".PASSENGER as pa on foi.PASSENGER_ID = pa.ID where fca.APPLY_NO=? and fca.CHANGE_STATUS=?";
//       String sql = "select pa.PASSENGER_DEPARTMENT_ID as passengerDepId ,pa.PASSENGER_STRUCTURE_ID as passengerSId ,pa.PASSENGER_COMPANY_ID as companyId ,foi.FLIGHT_ORDER_NO as orderNum ,pa.PASSENGER_TYPE as passengerType from FLIGHT_CHANGE_APPLY_ITEM as fcai　LEFT JOIN FLIGHT_CHANGE_APPLY as fca ON  fcai.APPLY_NO=fca.APPLY_NO　LEFT JOIN FLIGHT_ORDER_ITEM as foi ON fcai.NEW_ORDER_ITEM_ID =  foi.ID　LEFT JOIN PASSENGER as pa  on foi.PASSENGER_ID = pa.ID　where fca.APPLY_NO=? ";
//        List<OrderPassengerTick> orderPassengerTicks = jdbcTemplate.queryForList(sql,new String[]{changeNo},OrderPassengerTick.class);
        List<OrderPassengerTick> orderPassengerTicks =jdbcTemplate.query(sql,new String[]{changeNo,"06"}, new RowMapper() {
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
