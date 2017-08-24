package com.ly.report.tmc.datacollectionapi.dal.hive.db.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.RowMapperResultSetExtractor;
import org.springframework.stereotype.Repository;

import com.alibaba.fastjson.JSON;
import com.ly.report.tmc.datacollectionapi.dal.common.DalConstant;
import com.ly.report.tmc.datacollectionapi.dal.hive.db.HiveDAO;
import com.ly.report.tmc.datacollectionapi.dal.hive.db.to.OrderPassengerTick;

/**
 * Created by hyz46086 on 2017/4/19.
 */

@Repository
public class HiveDAOImpl implements HiveDAO {

    /** logger */
    private Logger            logger   = LoggerFactory.getLogger(HiveDAOImpl.class);

    private static final long pageSize = 5000L;

    @Resource
    JdbcTemplate              jdbcTemplate;

    @Override
    public List<Map<String, Object>> executeSQL(String sql, String... args) throws SQLException {
        List<Map<String, Object>> list = jdbcTemplate.query(sql, args, new RowMapperResultSetExtractor<Map<String, Object>>(new ColumnMapRowMapper()));
        return list;

    }

    @Override
    public List<Map<String, Object>> executeQuerySQL(String sql) throws SQLException {

        List<Map<String, Object>> list = jdbcTemplate.query(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
                PreparedStatement pstmt = con.prepareStatement(sql);
                return pstmt;
            }
        }, new RowMapperResultSetExtractor<Map<String, Object>>(new ColumnMapRowMapper()));
        return list;

    }

    @Override
    public void createTables(String sql) {

        this.jdbcTemplate.execute(sql);

    }

    @Override
    public List<Long> getCountTime(String tableName, String timeColumn, String... args) {

        long count = 0;

        /** 先查询结果数量 */
        StringBuilder querySql = new StringBuilder("select count(1) as allcount from ").append(DalConstant.DATABASE + "." + tableName).append(" where ")
            .append(timeColumn + " between ? and ?");
        //        String countSql ="select count(*) as allcount from"+tableName+" where OUT_TICKET_TIME=?" ;

        List<Map<String, Object>> result = null;//addParam(srcTable, querySql)
        try {
            result = executeQuerySQL(querySql.toString(), args);

        } catch (SQLException e) {
            logger.error("<reportcollectionapi><HiveDAOImpl><getCountTime><getCountTime><getCountTime>" + "【查询总数】查询总数，入参=" + JSON.toJSONString(args)
                         + ",结果********************************" + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
            e.printStackTrace();
        }
        if (CollectionUtils.isNotEmpty(result) && MapUtils.isNotEmpty(result.get(0))) {
            count = NumberUtils.toLong(ObjectUtils.toString(result.get(0).get("allcount")));
        }

        long totalTimes = count / pageSize + ((count % pageSize) > 0 ? 1 : 0);

        System.out.print("需要循环的次数" + totalTimes);
        List<Long> nums = new ArrayList<>();
        nums.add(0, count);
        nums.add(1, totalTimes);
        return nums;
    }

    @Override
    public Long getTotalTime(String tableName) {
        String sql = "select count(1) as allcount from " + DalConstant.DATABASE + "." + tableName;
        Long count = 0L;
        try {
            List<Map<String, Object>> list = executeQuerySQL(sql, null);
            if (list != null && list.size() > 0) {
                count = NumberUtils.toLong(ObjectUtils.toString(list.get(0).get("allcount")));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }

    @Override
    public List<Map<String, Object>> getResultByPage(int pageNum, String... args) {
        //        StringBuilder querySql = new StringBuilder("select GMT_CREATE as createTime,ORDER_NO as codeId,OUT_TICKET_TIME as finishTime, TMC_ID as tmcId from  FLIGHT_ORDER  where OUT_TICKET_TIME between ? and ?");
        List<Map<String, Object>> result = getCreatePageInfo(pageNum, "FLIGHT_ORDER", args);
        return result;
    }

    @Override
    public List<Map<String, Object>> getChangeResultByPage(int pageNum, String... args) {
        //        StringBuilder querySql = new StringBuilder("select GMT_CREATE as createTime, APPLY_NO as codeId,CHANGE_FINISH_DATE as finishTime, TMC_ID as tmcId from  FLIGHT_CHANGE_APPLY  where CHANGE_FINISH_DATE  between ? and ?");

        //        List<Map<String, Object>> result = getPageData(pageNum, querySql, args);
        List<Map<String, Object>> result = getChangePageInfo(pageNum, "FLIGHT_CHANGE_APPLY", args);
        return result;
    }

    @Override
    public List<Map<String, Object>> getRefundResultByPage(int pageNum, String... args) {
        //        StringBuilder querySql = new StringBuilder("select GMT_CREATE as createTime, APPLY_NO as codeId,REFUND_FINISH_DATE as finishTime, TMC_ID as tmcId from  FLIGHT_REFUND_APPLY  where REFUND_FINISH_DATE between ? and ?");

        //        List<Map<String, Object>> result = getPageData(pageNum, querySql, args);
        List<Map<String, Object>> result = getRefundPageInfo(pageNum, "FLIGHT_REFUND_APPLY", args);
        return result;
    }

    private List<Map<String, Object>> getPageData(int pageNum, StringBuilder querySql, String[] args) {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            //            result = executeQuerySQL(querySql.append(" limit ").append(Long.toString((pageNum - 1) * pageSize + 1)).append(" , ").append(Long.toString(pageNum * pageSize)).toString(),args);
            result = executeQuerySQL(querySql.append(" limit ").append(Long.toString(pageNum * pageSize)).toString(), args);
        } catch (SQLException e) {
            logger.error("<reportcollectionapi><HiveDAOImpl><getPageData><getPageData><getPageData>" + "【按页查询】按页查询，入参=" + JSON.toJSONString(args)
                         + ",异常********************************" + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
            e.printStackTrace();
        }
        return result;
    }

    public List<Map<String, Object>> getCreatePageInfo(int pageNum, String Table, String[] args) {
        StringBuilder querySql = new StringBuilder(
            "SELECT a.GMT_CREATE as createTime,a.ORDER_NO as codeId,a.OUT_TICKET_TIME as finishTime, a.TMC_ID as tmcId  FROM (SELECT *,row_number () over ( ORDER BY id DESC ) rank FROM ");
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            result = executeQuerySQL(querySql.append(DalConstant.DATABASE + "." + Table).append(")a where a.rank>= ").append(Long.toString((pageNum - 1) * pageSize + 1))
                .append(" and a.rank<= ").append(Long.toString(pageNum * pageSize)).append(" and OUT_TICKET_TIME between ? and ?").toString(), args);
        } catch (SQLException e) {
            logger.error("<reportcollectionapi><HiveDAOImpl><getCreatePageInfo><getCreatePageInfo><getCreatePageInfo>" + "【获取订单按页查询】获取订单按页查询，入参=" + JSON.toJSONString(args)
                         + ",异常********************************" + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
            e.printStackTrace();
        }
        return result;
    }

    public List<Map<String, Object>> getChangePageInfo(int pageNum, String Table, String[] args) {
        StringBuilder querySql = new StringBuilder(
            "SELECT a.GMT_CREATE as createTime, a.APPLY_NO as codeId,a.CHANGE_FINISH_DATE as finishTime, a.TMC_ID as tmcId  FROM (SELECT *,row_number () over ( ORDER BY id DESC ) rank FROM ");
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            result = executeQuerySQL(querySql.append(DalConstant.DATABASE + "." + Table).append(")a where a.rank>= ").append(Long.toString((pageNum - 1) * pageSize + 1))
                .append(" and a.rank<= ").append(Long.toString(pageNum * pageSize)).append(" and CHANGE_FINISH_DATE between ? and ?").toString(), args);
        } catch (SQLException e) {
            logger.error("<reportcollectionapi><HiveDAOImpl><getChangePageInfo><getChangePageInfo><getChangePageInfo>" + "【改签订单按页查询】改签订单按页查询，入参=" + JSON.toJSONString(args)
                         + ",结果********************************" + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
            e.printStackTrace();
        }
        return result;
    }

    public List<Map<String, Object>> getRefundPageInfo(int pageNum, String Table, String[] args) {
        StringBuilder querySql = new StringBuilder(
            "SELECT a.GMT_CREATE as createTime, a.APPLY_NO as codeId,a.REFUND_FINISH_DATE as finishTime, a.TMC_ID as tmcId  FROM (SELECT *,row_number () over ( ORDER BY id DESC ) rank FROM ");
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            result = executeQuerySQL(querySql.append(DalConstant.DATABASE + "." + Table).append(")a where a.rank>= ").append(Long.toString((pageNum - 1) * pageSize + 1))
                .append(" and a.rank<= ").append(Long.toString(pageNum * pageSize)).append(" and REFUND_FINISH_DATE between ? and ?").toString(), args);
        } catch (SQLException e) {
            logger.error("<reportcollectionapi><HiveDAOImpl><getRefundPageInfo><getRefundPageInfo><getRefundPageInfo>" + "【退票按页查询】退票按页查询，入参=" + JSON.toJSONString(args)
                         + ",异常********************************" + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public List<String> getOrderNumByPage(int pageNum) {

        return null;
    }

    @Override
    public List<OrderPassengerTick> getOrderItemByOrderNum(String orderNum) {
        logger.info("<reportcollectionapi><HiveDAOImpl><getOrderItemByOrderNum><getOrderItemByOrderNum><getOrderItemByOrderNum>" + "【根据订单号获取信息】根据订单号获取信息，入参="
                    + JSON.toJSONString(orderNum) + ",结果******coming---**************************");
        System.out.print("test");
        //        String sql  ="select DISTINCT foi.PASSENGER_ID as passengerId, foi.ID as itemId,pa.PASSENGER_DEPARTMENT_ID as passengerDepId ,pa.PASSENGER_STRUCTURE_ID as passengerSId ,pa.PASSENGER_COMPANY_ID as companyId ,foi.FLIGHT_ORDER_NO  as orderNum ,pa.PASSENGER_CLASS as passengerType  from "+DalConstant.DATABASE+".FLIGHT_ORDER_ITEM as foi  LEFT JOIN "+DalConstant.DATABASE+".PASSENGER as pa  on  foi.PASSENGER_ID = pa.ID  LEFT JOIN  "+DalConstant.DATABASE+".FLIGHT_TICKET as ft ON foi.FLIGHT_TICKET_ID = ft.ID where foi.FLIGHT_ORDER_NO = ? and ft.TICKET_TYPE=1 and foi.ITEM_TYPE=0";
        String sql = "select DISTINCT foi.PASSENGER_ID as passengerId, foi.ID as itemId,pa.PASSENGER_DEPARTMENT_ID as passengerDepId ,pa.PASSENGER_STRUCTURE_ID as passengerSId ,pa.PASSENGER_COMPANY_ID as companyId ,foi.FLIGHT_ORDER_NO  as orderNum ,pa.PASSENGER_CLASS as passengerType  from "
                     + DalConstant.DATABASE + ".FLIGHT_ORDER_ITEM as foi  LEFT JOIN " + DalConstant.DATABASE + ".PASSENGER as pa  on  foi.PASSENGER_ID = pa.ID  LEFT JOIN  "
                     + DalConstant.DATABASE + ".FLIGHT_TICKET as ft ON foi.FLIGHT_TICKET_ID = ft.ID where foi.FLIGHT_ORDER_NO = ? and foi.ITEM_TYPE=0";
        //        List<OrderPassengerTick> orderPassengerTicks = jdbcTemplate.queryForList(sql,new String[]{orderNum},OrderPassengerTick.class);
        List<OrderPassengerTick> orderPassengerTicks = jdbcTemplate.query(sql, new String[] { orderNum }, new RowMapper() {
            @Override
            public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                OrderPassengerTick orderPassengerTick = new OrderPassengerTick();
                orderPassengerTick.setPassengerId(rs.getString("passengerid"));
                orderPassengerTick.setItemId(rs.getString("itemid"));
                orderPassengerTick.setCompanyId(rs.getString("companyid"));
                orderPassengerTick.setOrderNum(rs.getString("ordernum"));
                orderPassengerTick.setPassengerDepId(rs.getString("passengerdepid"));
                orderPassengerTick.setPassengerSId(rs.getString("passengersid"));
                orderPassengerTick.setPassengerType(rs.getString("passengertype"));
                return orderPassengerTick;
            }
        });
        return orderPassengerTicks;
    }

    @Override
    public int[] batchInsertOrderDep(List<Object[]> args, String tableName) {
        try {
            logger.info("<reportcollectionapi><HiveDAOImpl><batchInsertOrderDep><batchInsertOrderDep><batchInsertOrderDep>【进入插入方法】进入插入方法" + tableName);
            String sql = "insert  into " + DalConstant.DATABASE + "." + tableName + "  values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            return jdbcTemplate.batchUpdate(sql, args);
        } catch (Exception e) {
            logger.error("<reportcollectionapi><HiveDAOImpl><batchInsertOrderDep><batchInsertOrderDep><batchInsertOrderDep>" + "【插入失败】插入失败，入参="
                         + ",异常********************************" + e.getStackTrace().toString());
            e.printStackTrace();
            return null;
        }

    }

    @Override
    public int isTopicDataExit(String... args) {

        int count = 0;
        String sql = "select count(1) as allcount from DEPARTMENT_ORDER_INFO where topic=? and order_status=? and validtime between ? and ? ";
        //        String sql = "select count(*) as allcount from DEPARTMENT_ORDER_INFO where orderStatus=? and createTime between ? and ? ";

        List<Map<String, Object>> result = null;//addParam(srcTable, querySql)
        try {
            result = executeQuerySQL(sql, args);
            ;
        } catch (SQLException e) {
            logger.error("<reportcollectionapi><HiveDAOImpl><isTopicDataExit><isTopicDataExit><isTopicDataExit>" + "【任务启动】启动任务，入参=" + JSON.toJSONString(args)
                         + ",异常********************************" + e.getStackTrace().toString());
            e.printStackTrace();
        }
        if (CollectionUtils.isNotEmpty(result) && MapUtils.isNotEmpty(result.get(0))) {
            count = NumberUtils.toInt(ObjectUtils.toString(result.get(0).get("allcount")));
        }

        return count;
    }

    @Override
    public void dropTable(String tableName) {
        String sql = "DROP TABLE IF EXISTS " + DalConstant.DATABASE + "." + tableName;
        jdbcTemplate.execute(sql);

    }

    @Override
    public void createTable(String tableName) {
        //       String sql =" CREATE TABLE IF NOT EXISTS "+DalConstant.DATABASE+"."+tableName+"(ID varchar(255),DEPARTMENT_ID BIGINT,ORDER_ID varchar(255), PASSENGER_ID BIGINT,PASSENGER_TYPE varchar(255) ,validTime string,TMC_ID BIGINT,COMPANY_ID BIGINT,ORDER_STATUS int ,BLOODRELATION int,itemId BIGINT,orderCreateTime string, topic string,messageId String,datetime string )";
        String sql = " CREATE TABLE IF NOT EXISTS " + DalConstant.DATABASE + "." + tableName
                     + "(PARENT_ID BIGINT, DEPARTMENT_ID BIGINT,ORDER_ID varchar(255), PASSENGER_ID BIGINT,PASSENGER_TYPE varchar(255) ,validTime string,TMC_ID BIGINT,ORDER_STATUS int ,BLOODRELATION int,itemId BIGINT,orderCreateTime string,datetime string,COMPANY_ID BIGINT )";
        createTables(sql);
    }

    @Override
    public void batchInsertOrderByPartition(Object[] args) {

        //       String dynamic =  "set hive.exec.dynamic.partition=true";
        //        String nons ="set hive.exec.dynamic.partition.mode=nonstrict";
        //        jdbcTemplate.execute(dynamic);
        //        jdbcTemplate.execute(nons);
        //        String sql="insert INTO table  "+DalConstant.DATABASE+".DEPARTMENT_ORDER_INFO_partition  partition (dateTime="+args[0]+",ORDER_STATUS="+args[1]+") select ID, DEPARTMENT_ID,ORDER_ID,PASSENGER_ID,PASSENGER_TYPE,validTime,TMC_ID,COMPANY_ID,BLOODRELATION,itemId,orderCreateTime,topic,messageId from "+DalConstant.DATABASE+"."+args[2];
        String sql = "insert overwrite table  " + DalConstant.DATABASE + ".DEPARTMENT_ORDER_INFO_partition  partition (dateTime=" + args[0]
                     + ") select parent_id,DEPARTMENT_ID,ORDER_ID,PASSENGER_ID,PASSENGER_TYPE,validTime,TMC_ID,COMPANY_ID,ORDER_STATUS,BLOODRELATION,itemId,orderCreateTime FROM "
                     + DalConstant.DATABASE + "." + args[1];
        //        String sql="insert overwrite table DEPARTMENT_ORDER_INFO_partition  partition (dateTime,ORDER_STATU) select ID, DEPARTMENT_ID,ORDER_ID,PASSENGER_ID,PASSENGER_TYPE,validTime,TMC_ID,COMPANY_ID,BLOODRELATION,itemId,orderCreateTime,topic,messageId,datetime,order_status from "+args[2];

        jdbcTemplate.execute(sql);
    }

    @Override
    public void deletePartition(String partition) {
        String sql = "ALTER TABLE " + DalConstant.DATABASE + ".DEPARTMENT_ORDER_INFO_partition DROP IF EXISTS PARTITION(dateTime='" + partition + "')";
        this.jdbcTemplate.execute(sql);
    }

    @Override
    public Long getUpdateCountTime(String sql, String... args) {
        //        String sql = " select count(1) as allcount,DISTINCT foi.PASSENGER_ID as passengerId  from "+DalConstant.DATABASE+".FLIGHT_ORDER_ITEM as foi  LEFT JOIN "+DalConstant.DATABASE+".PASSENGER as pa  on  foi.PASSENGER_ID = pa.ID LEFT JOIN "+DalConstant.DATABASE+".FLIGHT_ORDER as fo on foi.FLIGHT_ORDER_NO = fo.ID  LEFT JOIN  "+DalConstant.DATABASE+".FLIGHT_TICKET as ft ON foi.FLIGHT_TICKET_ID = ft.ID where ft.TICKET_STATUS='A' and foi.ITEM_TYPE=0 and  fo."+timeColumn+" between ? and ?";

        Long count = 0L;
        List<Map<String, Object>> result = null;
        try {
            result = executeQuerySQL(sql, args);

        } catch (SQLException e) {
            logger.error("<reportcollectionapi><HiveDAOImpl><getUpdateCountTime><getUpdateCountTime><getUpdateCountTime>" + "【查询总数】查询总数，入参=" + JSON.toJSONString(args)
                         + ",结果********************************" + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
            e.printStackTrace();
        }
        if (CollectionUtils.isNotEmpty(result) && MapUtils.isNotEmpty(result.get(0))) {
            count = NumberUtils.toLong(ObjectUtils.toString(result.get(0).get("allcount")));
        }
        long totalBatch = count / pageSize + ((count % pageSize) > 0 ? 1 : 0);

        return totalBatch;
    }

    @Override
    public void getOrderPassengerTickBatch(int batchNum, String tableName, String... args) {

        //    public List<OrderPassengerTickUpdate> getOrderPassengerTickBatch(int batchNum,String tableName,String ...args) {

        //        String sql = "select DISTINCT a.PASSENGER_ID as passengerId, a.GMT_CREATE as createTime,a.ORDER_NO as codeId,a.OUT_TICKET_TIME as finishTime, a.TMC_ID as tmcId ,a.ID as itemId,a.PASSENGER_DEPARTMENT_ID as passengerDepId ,a.PASSENGER_STRUCTURE_ID as passengerSId ,a.PASSENGER_COMPANY_ID as companyId ,a.FLIGHT_ORDER_NO  as orderNum ,a.PASSENGER_CLASS as passengerType (select * ,row_number () over ( ORDER BY id DESC ) rank FROM  "+DalConstant.DATABASE+".FLIGHT_ORDER_ITEM as foi  LEFT JOIN "+DalConstant.DATABASE+".PASSENGER as pa  on  foi.PASSENGER_ID = pa.ID LEFT JOIN "+DalConstant.DATABASE+".FLIGHT_ORDER as fo on foi.FLIGHT_ORDER_NO = fo.ID LEFT JOIN  "+DalConstant.DATABASE+".FLIGHT_TICKET as ft ON foi.FLIGHT_TICKET_ID = ft.ID where ft.TICKET_STATUS='A' and foi.ITEM_TYPE=0) as a  ";
        //        String sql = "select DISTINCT passengerId,createTime,codeId, finishTime,tmcId ,itemId,passengerDepId , passengerSId ,companyId ,orderNum , passengerType FROM (select DISTINCT foi.PASSENGER_ID as passengerId, fo.GMT_CREATE as createTime,fo.ORDER_NO as codeId,fo.OUT_TICKET_TIME as finishTime,fo.TMC_ID as tmcId ,foi.ID as itemId,pa.PASSENGER_DEPARTMENT_ID as passengerDepId ,pa.PASSENGER_STRUCTURE_ID as passengerSId ,pa.PASSENGER_COMPANY_ID as companyId ,foi.FLIGHT_ORDER_NO  as orderNum ,pa.PASSENGER_CLASS as passengerType ,foi.ITEM_TYPE as item_type ,row_number () over ( ORDER BY fo.ORDER_NO DESC ) rank FROM  "+DalConstant.DATABASE+".FLIGHT_ORDER_ITEM as foi  LEFT JOIN "+DalConstant.DATABASE+".PASSENGER as pa  on  foi.PASSENGER_ID = pa.ID LEFT JOIN "+DalConstant.DATABASE+".FLIGHT_ORDER as fo on foi.FLIGHT_ORDER_NO = fo.ORDER_NO LEFT JOIN  "+DalConstant.DATABASE+".FLIGHT_TICKET as ft ON foi.FLIGHT_TICKET_ID = ft.ID where foi.ITEM_TYPE=0) as a  ";
        //        String createTable = " CREATE TABLE IF NOT EXISTS "+DalConstant.DATABASE+"."+tableName+" row format delimited fields terminated by '\t' as ";
        String insertTable = " insert into " + DalConstant.DATABASE + "." + tableName;
        StringBuilder querySql = new StringBuilder(insertTable);
        /*String sql = " select a.passengerDepId,a.ORDER_ID, a.PASSENGER_ID, a.PASSENGER_TYPE ,a.validTime,a.tmcId ,a.ORDER_STATUS,(CASE WHEN passengerDepId =  TOP_ID THEN 1 ELSE 0 END) BLOODRELATION,a.itemId,a.orderCreateTime,a.datetime,a.companyId  FROM ("
                     + "select foi.FLIGHT_ORDER_NO as ORDER_ID, foi.PASSENGER_ID as PASSENGER_ID, pa.PASSENGER_CLASS as PASSENGER_TYPE ,ft.TICKET_STATUS as TICKET_STATUS, fo.OUT_TICKET_TIME as validTime, fo.TMC_ID as tmcId ,0 ORDER_STATUS,"
                     + "(CASE WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 0 THEN PASSENGER_STRUCTURE_ID "
                     + " WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 1 AND substring(PASSENGER_STRUCTURE_ID,1,1)=','   THEN substring(PASSENGER_STRUCTURE_ID,2) "
                     + "WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 1 AND substring(PASSENGER_STRUCTURE_ID,1,1)<>','  THEN regexp_extract(PASSENGER_STRUCTURE_ID, '(.*?)(,)(.*?)', 1) "
                     + "WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) > 1 AND substring(PASSENGER_STRUCTURE_ID,1,1) =','  THEN regexp_extract(PASSENGER_STRUCTURE_ID, ',(.*?)(,)(.*?)', 1) "
                     + "WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) > 1 AND substring(PASSENGER_STRUCTURE_ID,1,1) <>',' THEN regexp_extract(PASSENGER_STRUCTURE_ID, '(.*?)(,)(.*?)', 1)  END) TOP_ID"
                     + ",foi.ID as itemId,fo.GMT_CREATE as orderCreateTime,'" + args[0].split(" ")[0]
                     + "' datetime,(CASE WHEN PASSENGER_STRUCTURE_ID ='' THEN PASSENGER_DEPARTMENT_ID ELSE PASSENGER_STRUCTURE_ID END ) as passengerSId ,pa.PASSENGER_COMPANY_ID as companyId ,foi.ITEM_TYPE as item_type,row_number () over ( ORDER BY fo.ORDER_NO DESC ) rank FROM  "
                     + DalConstant.DATABASE + ".FLIGHT_ORDER_ITEM as foi  LEFT JOIN " + DalConstant.DATABASE + ".PASSENGER as pa  on  foi.PASSENGER_ID = pa.ID LEFT JOIN "
                     + DalConstant.DATABASE + ".FLIGHT_ORDER as fo on foi.FLIGHT_ORDER_NO = fo.ORDER_NO LEFT JOIN  " + DalConstant.DATABASE
                     + ".FLIGHT_TICKET as ft ON foi.FLIGHT_TICKET_ID = ft.ID) as a  ";
        //        String sql = " select *  FROM (select foi.PASSENGER_ID as passengerId, fo.GMT_CREATE as createTime,fo.ORDER_NO as codeId,fo.OUT_TICKET_TIME as finishTime,fo.TMC_ID as tmcId ,foi.ID as itemId,pa.PASSENGER_DEPARTMENT_ID as passengerDepId ,pa.PASSENGER_STRUCTURE_ID as passengerSId ,pa.PASSENGER_COMPANY_ID as companyId ,foi.FLIGHT_ORDER_NO  as orderNum ,pa.PASSENGER_CLASS as passengerType ,foi.ITEM_TYPE as item_type ,row_number () over ( ORDER BY fo.ORDER_NO DESC ) rank FROM  "+DalConstant.DATABASE+".FLIGHT_ORDER_ITEM as foi  LEFT JOIN "+DalConstant.DATABASE+".PASSENGER as pa  on  foi.PASSENGER_ID = pa.ID LEFT JOIN "+DalConstant.DATABASE+".FLIGHT_ORDER as fo on foi.FLIGHT_ORDER_NO = fo.ORDER_NO LEFT JOIN  "+DalConstant.DATABASE+".FLIGHT_TICKET as ft ON foi.FLIGHT_TICKET_ID = ft.ID) as a  ";
        //        querySql.append(sql).append(" lateral view explode(split(a.passengerSId, ',')) a as passengerDepId ").append(" where a.rank>= ").append(Long.toString((batchNum - 1) * pageSize + 1)).append(" and a.rank<= ").append(Long.toString(batchNum * pageSize)).append(" and a.item_type=0 and a.TICKET_STATUS='A' and a.validTime between '"+args[0]+"' and '"+args[1]+"'" );
        querySql.append(sql)
            .append(
                " lateral view explode(split((CASE WHEN substring(a.passengerSId,1,1)=',' THEN substring(a.passengerSId,2) ELSE a.passengerSId END), ',')) a as passengerDepId ")
            .append(" where a.rank>= ").append(Long.toString((batchNum - 1) * pageSize + 1)).append(" and a.rank<= ").append(Long.toString(batchNum * pageSize))
            .append(" and a.item_type=0 and a.validTime between '" + args[0] + "' and '" + args[1] + "'");
        this.jdbcTemplate.execute(querySql.toString());*/

        String sql = " SELECT (CASE WHEN a.PARENT_ID = '' THEN -99 ELSE a.PARENT_ID  END ) PARENT_ID "
                     + " ,a.passengerDepId department_id,a.ORDER_ID, a.PASSENGER_ID, a.PASSENGER_TYPE ,a.validTime,a.tmcId ,a.ORDER_STATUS,BLOODRELATION,a.itemId,a.orderCreateTime,a.datetime,a.companyId FROM ( "
                     + " SELECT a.DEP_S_ID, reverse(regexp_replace( reverse(regexp_replace(a.DEP_S_ID,concat(',',substr(a.DEP_S_ID,instr(a.DEP_S_ID,a.passengerDepId))),'')), "
                     + "  substr( reverse(regexp_replace(a.DEP_S_ID,concat(',',substr(a.DEP_S_ID,instr(a.DEP_S_ID,a.passengerDepId))),'')), instr( "
                     + "    reverse(regexp_replace(a.DEP_S_ID,concat(',',substr(a.DEP_S_ID,instr(a.DEP_S_ID,a.passengerDepId))),'')),','   ) ) ,''  ) ) PARENT_ID "
                     + ",a.passengerDepId,a.ORDER_ID, a.PASSENGER_ID, a.PASSENGER_TYPE ,a.validTime,a.tmcId ,a.ORDER_STATUS,(CASE WHEN passengerDepId =  TOP_ID THEN 1 ELSE 0 END) BLOODRELATION,a.itemId,a.orderCreateTime,a.datetime,a.companyId  FROM ("
                     + "select foi.FLIGHT_ORDER_NO as ORDER_ID, foi.PASSENGER_ID as PASSENGER_ID, pa.PASSENGER_CLASS as PASSENGER_TYPE ,ft.TICKET_STATUS as TICKET_STATUS, fo.OUT_TICKET_TIME as validTime, fo.TMC_ID as tmcId ,0 ORDER_STATUS,"
                     + "(CASE WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 0 THEN PASSENGER_STRUCTURE_ID "
                     + " WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 1 AND substring(PASSENGER_STRUCTURE_ID,1,1)=','   THEN substring(PASSENGER_STRUCTURE_ID,2) "
                     + "WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 1 AND substring(PASSENGER_STRUCTURE_ID,1,1)<>','  THEN regexp_extract(PASSENGER_STRUCTURE_ID, '(.*?)(,)(.*?)', 1) "
                     + "WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) > 1 AND substring(PASSENGER_STRUCTURE_ID,1,1) =','  THEN regexp_extract(PASSENGER_STRUCTURE_ID, ',(.*?)(,)(.*?)', 1) "
                     + "WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) > 1 AND substring(PASSENGER_STRUCTURE_ID,1,1) <>',' THEN regexp_extract(PASSENGER_STRUCTURE_ID, '(.*?)(,)(.*?)', 1)  END) TOP_ID"
                     + "  ,(CASE WHEN PASSENGER_STRUCTURE_ID='' THEN CONCAT(',',PASSENGER_DEPARTMENT_ID) ELSE ( CASE WHEN substring(PASSENGER_STRUCTURE_ID,1,1)=','THEN PASSENGER_STRUCTURE_ID ELSE CONCAT(',',PASSENGER_STRUCTURE_ID) END ) END  ) DEP_S_ID "
                     + ",foi.ID as itemId,fo.GMT_CREATE as orderCreateTime,'" + args[0].split(" ")[0]
                     + "' datetime,(CASE WHEN PASSENGER_STRUCTURE_ID ='' THEN PASSENGER_DEPARTMENT_ID ELSE PASSENGER_STRUCTURE_ID END ) as passengerSId ,pa.PASSENGER_COMPANY_ID as companyId ,foi.ITEM_TYPE as item_type,row_number () over ( ORDER BY fo.ORDER_NO DESC ) rank FROM  "
                     + DalConstant.DATABASE + ".FLIGHT_ORDER_ITEM as foi  LEFT JOIN " + DalConstant.DATABASE + ".PASSENGER as pa  on  foi.PASSENGER_ID = pa.ID LEFT JOIN "
                     + DalConstant.DATABASE + ".FLIGHT_ORDER as fo on foi.FLIGHT_ORDER_NO = fo.ORDER_NO LEFT JOIN  " + DalConstant.DATABASE
                     + ".FLIGHT_TICKET as ft ON foi.FLIGHT_TICKET_ID = ft.ID) as a  ";
        //        String sql = " select *  FROM (select foi.PASSENGER_ID as passengerId, fo.GMT_CREATE as createTime,fo.ORDER_NO as codeId,fo.OUT_TICKET_TIME as finishTime,fo.TMC_ID as tmcId ,foi.ID as itemId,pa.PASSENGER_DEPARTMENT_ID as passengerDepId ,pa.PASSENGER_STRUCTURE_ID as passengerSId ,pa.PASSENGER_COMPANY_ID as companyId ,foi.FLIGHT_ORDER_NO  as orderNum ,pa.PASSENGER_CLASS as passengerType ,foi.ITEM_TYPE as item_type ,row_number () over ( ORDER BY fo.ORDER_NO DESC ) rank FROM  "+DalConstant.DATABASE+".FLIGHT_ORDER_ITEM as foi  LEFT JOIN "+DalConstant.DATABASE+".PASSENGER as pa  on  foi.PASSENGER_ID = pa.ID LEFT JOIN "+DalConstant.DATABASE+".FLIGHT_ORDER as fo on foi.FLIGHT_ORDER_NO = fo.ORDER_NO LEFT JOIN  "+DalConstant.DATABASE+".FLIGHT_TICKET as ft ON foi.FLIGHT_TICKET_ID = ft.ID) as a  ";
        //        querySql.append(sql).append(" lateral view explode(split(a.passengerSId, ',')) a as passengerDepId ").append(" where a.rank>= ").append(Long.toString((batchNum - 1) * pageSize + 1)).append(" and a.rank<= ").append(Long.toString(batchNum * pageSize)).append(" and a.item_type=0 and a.TICKET_STATUS='A' and a.validTime between '"+args[0]+"' and '"+args[1]+"'" );
        querySql.append(sql)
            .append(
                " lateral view explode(split((CASE WHEN substring(a.passengerSId,1,1)=',' THEN substring(a.passengerSId,2) ELSE a.passengerSId END), ',')) a as passengerDepId ")
            .append(" where a.rank>= ").append(Long.toString((batchNum - 1) * pageSize + 1)).append(" and a.rank<= ").append(Long.toString(batchNum * pageSize))
            .append(" and a.item_type=0 and a.validTime between '" + args[0] + "' and '" + args[1] + "'" + ") a");
        this.jdbcTemplate.execute(querySql.toString());
    }

    @Override
    public void insertChangeData(int batchNum, String tableName, String... args) {
        String insertTable = " insert into " + DalConstant.DATABASE + "." + tableName;
        StringBuilder querySql = new StringBuilder(insertTable);
        /*String sql = " select a.passengerDepId,a.ORDER_ID, a.PASSENGER_ID, a.PASSENGER_TYPE ,a.validTime,a.tmcId ,a.ORDER_STATUS,(CASE WHEN passengerDepId =  TOP_ID THEN 1 ELSE 0 END) BLOODRELATION,a.itemId,a.orderCreateTime,a.datetime,a.companyId  FROM ("
                     + "select foi.PASSENGER_ID as PASSENGER_ID,fca.GMT_CREATE as orderCreateTime,'" + args[0].split(" ")[0]
                     + "' datetime,fca.CHANGE_FINISH_DATE as validTime,2 ORDER_STATUS,"
                     + "(CASE WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 0 THEN PASSENGER_STRUCTURE_ID "
                     + " WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 1 AND substring(PASSENGER_STRUCTURE_ID,1,1)=','   THEN substring(PASSENGER_STRUCTURE_ID,2) "
                     + "WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 1 AND substring(PASSENGER_STRUCTURE_ID,1,1)<>','  THEN regexp_extract(PASSENGER_STRUCTURE_ID, '(.*?)(,)(.*?)', 1) "
                     + "WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) > 1 AND substring(PASSENGER_STRUCTURE_ID,1,1) =','  THEN regexp_extract(PASSENGER_STRUCTURE_ID, ',(.*?)(,)(.*?)', 1) "
                     + "WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) > 1 AND substring(PASSENGER_STRUCTURE_ID,1,1) <>',' THEN regexp_extract(PASSENGER_STRUCTURE_ID, '(.*?)(,)(.*?)', 1)  END) TOP_ID"
                     + ",fca.TMC_ID as tmcId ,pa.PASSENGER_DEPARTMENT_ID as passengerDepId,foi.ID as itemId,(CASE WHEN PASSENGER_STRUCTURE_ID ='' THEN PASSENGER_DEPARTMENT_ID ELSE PASSENGER_STRUCTURE_ID END ) as passengerSId,pa.PASSENGER_COMPANY_ID as companyId,foi.FLIGHT_ORDER_NO as ORDER_ID,pa.PASSENGER_CLASS as PASSENGER_TYPE,fca.CHANGE_STATUS as CHANGE_STATUS,row_number () over ( ORDER BY foi.FLIGHT_ORDER_NO DESC ) rank From "
                     + DalConstant.DATABASE + ".FLIGHT_CHANGE_APPLY_ITEM as fcai LEFT JOIN " + DalConstant.DATABASE
                     + ".FLIGHT_CHANGE_APPLY as fca ON fcai.APPLY_NO=fca.APPLY_NO LEFT JOIN " + DalConstant.DATABASE
                     + ".FLIGHT_ORDER_ITEM as foi ON fcai.NEW_ORDER_ITEM_ID = foi.ID LEFT JOIN " + DalConstant.DATABASE + ".PASSENGER as pa on foi.PASSENGER_ID = pa.ID) as a ";
        querySql.append(sql)
            .append(
                " lateral view explode(split((CASE WHEN substring(a.passengerSId,1,1)=',' THEN substring(a.passengerSId,2) ELSE a.passengerSId END), ',')) a as passengerDepId ")
            .append(" where a.rank>= ").append(Long.toString((batchNum - 1) * pageSize + 1)).append(" and a.rank<= ").append(Long.toString(batchNum * pageSize))
            .append(" and a.CHANGE_STATUS IN ('06',07) and a.validTime between '" + args[0] + "' and '" + args[1] + "'");
        this.jdbcTemplate.execute(querySql.toString());*/

        String sql = " SELECT (CASE WHEN a.PARENT_ID = '' THEN -99 ELSE a.PARENT_ID  END ) PARENT_ID "
                     + " ,a.passengerDepId department_id,a.ORDER_ID, a.PASSENGER_ID, a.PASSENGER_TYPE ,a.validTime,a.tmcId ,a.ORDER_STATUS,BLOODRELATION,a.itemId,a.orderCreateTime,a.datetime,a.companyId FROM ( "
                     + " SELECT a.DEP_S_ID, reverse(regexp_replace( reverse(regexp_replace(a.DEP_S_ID,concat(',',substr(a.DEP_S_ID,instr(a.DEP_S_ID,a.passengerDepId))),'')), "
                     + "  substr( reverse(regexp_replace(a.DEP_S_ID,concat(',',substr(a.DEP_S_ID,instr(a.DEP_S_ID,a.passengerDepId))),'')), instr( "
                     + "    reverse(regexp_replace(a.DEP_S_ID,concat(',',substr(a.DEP_S_ID,instr(a.DEP_S_ID,a.passengerDepId))),'')),','   ) ) ,''  ) ) PARENT_ID "

                     + ",a.passengerDepId,a.ORDER_ID, a.PASSENGER_ID, a.PASSENGER_TYPE ,a.validTime,a.tmcId ,a.ORDER_STATUS,(CASE WHEN passengerDepId =  TOP_ID THEN 1 ELSE 0 END) BLOODRELATION,a.itemId,a.orderCreateTime,a.datetime,a.companyId  FROM ("
                     + "select foi.PASSENGER_ID as PASSENGER_ID,fca.GMT_CREATE as orderCreateTime,'" + args[0].split(" ")[0]
                     + "' datetime,fca.CHANGE_FINISH_DATE as validTime,2 ORDER_STATUS,"
                     + "(CASE WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 0 THEN PASSENGER_STRUCTURE_ID "
                     + " WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 1 AND substring(PASSENGER_STRUCTURE_ID,1,1)=','   THEN substring(PASSENGER_STRUCTURE_ID,2) "
                     + "WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 1 AND substring(PASSENGER_STRUCTURE_ID,1,1)<>','  THEN regexp_extract(PASSENGER_STRUCTURE_ID, '(.*?)(,)(.*?)', 1) "
                     + "WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) > 1 AND substring(PASSENGER_STRUCTURE_ID,1,1) =','  THEN regexp_extract(PASSENGER_STRUCTURE_ID, ',(.*?)(,)(.*?)', 1) "
                     + "WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) > 1 AND substring(PASSENGER_STRUCTURE_ID,1,1) <>',' THEN regexp_extract(PASSENGER_STRUCTURE_ID, '(.*?)(,)(.*?)', 1)  END) TOP_ID"
                     + "  ,(CASE WHEN PASSENGER_STRUCTURE_ID='' THEN CONCAT(',',PASSENGER_DEPARTMENT_ID) ELSE ( CASE WHEN substring(PASSENGER_STRUCTURE_ID,1,1)=','THEN PASSENGER_STRUCTURE_ID ELSE CONCAT(',',PASSENGER_STRUCTURE_ID) END ) END  ) DEP_S_ID "
                     + ",fca.TMC_ID as tmcId ,pa.PASSENGER_DEPARTMENT_ID as passengerDepId,foi.ID as itemId,(CASE WHEN PASSENGER_STRUCTURE_ID ='' THEN PASSENGER_DEPARTMENT_ID ELSE PASSENGER_STRUCTURE_ID END ) as passengerSId,pa.PASSENGER_COMPANY_ID as companyId,foi.FLIGHT_ORDER_NO as ORDER_ID,pa.PASSENGER_CLASS as PASSENGER_TYPE,fca.CHANGE_STATUS as CHANGE_STATUS,row_number () over ( ORDER BY foi.FLIGHT_ORDER_NO DESC ) rank From "
                     + DalConstant.DATABASE + ".FLIGHT_CHANGE_APPLY_ITEM as fcai LEFT JOIN " + DalConstant.DATABASE
                     + ".FLIGHT_CHANGE_APPLY as fca ON fcai.APPLY_NO=fca.APPLY_NO LEFT JOIN " + DalConstant.DATABASE
                     + ".FLIGHT_ORDER_ITEM as foi ON fcai.NEW_ORDER_ITEM_ID = foi.ID LEFT JOIN " + DalConstant.DATABASE + ".PASSENGER as pa on foi.PASSENGER_ID = pa.ID) as a ";
        querySql.append(sql)
            .append(
                " lateral view explode(split((CASE WHEN substring(a.passengerSId,1,1)=',' THEN substring(a.passengerSId,2) ELSE a.passengerSId END), ',')) a as passengerDepId ")
            .append(" where a.rank>= ").append(Long.toString((batchNum - 1) * pageSize + 1)).append(" and a.rank<= ").append(Long.toString(batchNum * pageSize))
            .append(" and a.CHANGE_STATUS IN ('01','06',07) and a.validTime between '" + args[0] + "' and '" + args[1] + "'" + ") a");
        this.jdbcTemplate.execute(querySql.toString());

    }

    @Override
    public void insertRefundData(int batchNum, String tableName, String... args) {
        String insertTable = " insert into " + DalConstant.DATABASE + "." + tableName;
        StringBuilder querySql = new StringBuilder(insertTable);
        /*String sql = " select a.passengerDepId,a.ORDER_ID, a.PASSENGER_ID, a.PASSENGER_TYPE ,a.validTime,a.tmcId ,a.ORDER_STATUS,(CASE WHEN passengerDepId =  TOP_ID THEN 1 ELSE 0 END) BLOODRELATION,a.itemId,a.orderCreateTime,a.datetime,a.companyId  FROM ("
                     + "SELECT foi.PASSENGER_ID as PASSENGER_ID,fra.GMT_CREATE as orderCreateTime,'" + args[0].split(" ")[0]
                     + "' datetime,fra.APPLY_NO as codeId,fra.REFUND_FINISH_DATE as validTime,1 ORDER_STATUS,"
                     + "(CASE WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 0 THEN PASSENGER_STRUCTURE_ID "
                     + " WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 1 AND substring(PASSENGER_STRUCTURE_ID,1,1)=','   THEN substring(PASSENGER_STRUCTURE_ID,2) "
                     + "WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 1 AND substring(PASSENGER_STRUCTURE_ID,1,1)<>','  THEN regexp_extract(PASSENGER_STRUCTURE_ID, '(.*?)(,)(.*?)', 1) "
                     + "WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) > 1 AND substring(PASSENGER_STRUCTURE_ID,1,1) =','  THEN regexp_extract(PASSENGER_STRUCTURE_ID, ',(.*?)(,)(.*?)', 1) "
                     + "WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) > 1 AND substring(PASSENGER_STRUCTURE_ID,1,1) <>',' THEN regexp_extract(PASSENGER_STRUCTURE_ID, '(.*?)(,)(.*?)', 1)  END) TOP_ID"
                     + ",fra.TMC_ID as tmcId,pa.ID as passengerId,foi.ID as itemId,(CASE WHEN PASSENGER_STRUCTURE_ID ='' THEN PASSENGER_DEPARTMENT_ID ELSE PASSENGER_STRUCTURE_ID END ) as passengersId,pa.PASSENGER_COMPANY_ID as companyId,foi.FLIGHT_ORDER_NO as ORDER_ID ,pa.PASSENGER_CLASS as PASSENGER_TYPE,fra.REFUND_STATUS as REFUND_STATUS,row_number () over ( ORDER BY foi.FLIGHT_ORDER_NO DESC ) rank From "
                     + DalConstant.DATABASE + ".FLIGHT_REFUND_APPLY_ITEM as frai LEFT JOIN " + DalConstant.DATABASE
                     + ".FLIGHT_REFUND_APPLY as fra ON frai.FLIGHT_REFUND_APPLY_NO = fra.APPLY_NO LEFT JOIN " + DalConstant.DATABASE
                     + ".FLIGHT_ORDER_ITEM as foi ON frai.ORDER_ITEM_ID=foi.ID LEFT JOIN " + DalConstant.DATABASE + ".PASSENGER as pa on foi.PASSENGER_ID=pa.ID ) as a ";
        querySql.append(sql)
            .append(
                " lateral view explode(split((CASE WHEN substring(a.passengerSId,1,1)=',' THEN substring(a.passengerSId,2) ELSE a.passengerSId END), ',')) a as passengerDepId ")
            .append(" where a.rank>= ").append(Long.toString((batchNum - 1) * pageSize + 1)).append(" and a.rank<= ").append(Long.toString(batchNum * pageSize))
            .append(" and a.REFUND_STATUS IN ('02','03') and a.validTime between '" + args[0] + "' and '" + args[1] + "'");
        this.jdbcTemplate.execute(querySql.toString());*/

        String sql = " SELECT ( CASE WHEN a.PARENT_ID = '' THEN -99 ELSE a.PARENT_ID  END ) PARENT_ID "
                     + " ,a.passengerDepId department_id,a.ORDER_ID, a.PASSENGER_ID, a.PASSENGER_TYPE ,a.validTime,a.tmcId ,a.ORDER_STATUS,BLOODRELATION,a.itemId,a.orderCreateTime,a.datetime,a.companyId FROM ( "
                     + " SELECT a.DEP_S_ID, reverse(regexp_replace( reverse(regexp_replace(a.DEP_S_ID,concat(',',substr(a.DEP_S_ID,instr(a.DEP_S_ID,a.passengerDepId))),'')), "
                     + "  substr( reverse(regexp_replace(a.DEP_S_ID,concat(',',substr(a.DEP_S_ID,instr(a.DEP_S_ID,a.passengerDepId))),'')), instr( "
                     + "    reverse(regexp_replace(a.DEP_S_ID,concat(',',substr(a.DEP_S_ID,instr(a.DEP_S_ID,a.passengerDepId))),'')),','   ) ) ,''  ) ) PARENT_ID "

                     + ",a.passengerDepId,a.ORDER_ID, a.PASSENGER_ID, a.PASSENGER_TYPE ,a.validTime,a.tmcId ,a.ORDER_STATUS,(CASE WHEN passengerDepId =  TOP_ID THEN 1 ELSE 0 END) BLOODRELATION,a.itemId,a.orderCreateTime,a.datetime,a.companyId  FROM ("
                     + "SELECT foi.PASSENGER_ID as PASSENGER_ID,fra.GMT_CREATE as orderCreateTime,'" + args[0].split(" ")[0]
                     + "' datetime,fra.APPLY_NO as codeId,fra.REFUND_FINISH_DATE as validTime,1 ORDER_STATUS,"
                     + "(CASE WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 0 THEN PASSENGER_STRUCTURE_ID "
                     + " WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 1 AND substring(PASSENGER_STRUCTURE_ID,1,1)=','   THEN substring(PASSENGER_STRUCTURE_ID,2) "
                     + "WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 1 AND substring(PASSENGER_STRUCTURE_ID,1,1)<>','  THEN regexp_extract(PASSENGER_STRUCTURE_ID, '(.*?)(,)(.*?)', 1) "
                     + "WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) > 1 AND substring(PASSENGER_STRUCTURE_ID,1,1) =','  THEN regexp_extract(PASSENGER_STRUCTURE_ID, ',(.*?)(,)(.*?)', 1) "
                     + "WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) > 1 AND substring(PASSENGER_STRUCTURE_ID,1,1) <>',' THEN regexp_extract(PASSENGER_STRUCTURE_ID, '(.*?)(,)(.*?)', 1)  END) TOP_ID"
                     + "  ,(CASE WHEN PASSENGER_STRUCTURE_ID='' THEN CONCAT(',',PASSENGER_DEPARTMENT_ID) ELSE ( CASE WHEN substring(PASSENGER_STRUCTURE_ID,1,1)=','THEN PASSENGER_STRUCTURE_ID ELSE CONCAT(',',PASSENGER_STRUCTURE_ID) END ) END  ) DEP_S_ID "

                     + ",fra.TMC_ID as tmcId,pa.ID as passengerId,foi.ID as itemId,(CASE WHEN PASSENGER_STRUCTURE_ID ='' THEN PASSENGER_DEPARTMENT_ID ELSE PASSENGER_STRUCTURE_ID END ) as passengersId,pa.PASSENGER_COMPANY_ID as companyId,foi.FLIGHT_ORDER_NO as ORDER_ID ,pa.PASSENGER_CLASS as PASSENGER_TYPE,fra.REFUND_STATUS as REFUND_STATUS,row_number () over ( ORDER BY foi.FLIGHT_ORDER_NO DESC ) rank From "
                     + DalConstant.DATABASE + ".FLIGHT_REFUND_APPLY_ITEM as frai LEFT JOIN " + DalConstant.DATABASE
                     + ".FLIGHT_REFUND_APPLY as fra ON frai.FLIGHT_REFUND_APPLY_NO = fra.APPLY_NO LEFT JOIN " + DalConstant.DATABASE
                     + ".FLIGHT_ORDER_ITEM as foi ON frai.ORDER_ITEM_ID=foi.ID LEFT JOIN " + DalConstant.DATABASE + ".PASSENGER as pa on foi.PASSENGER_ID=pa.ID ) as a ";
        querySql.append(sql)
            .append(
                " lateral view explode(split((CASE WHEN substring(a.passengerSId,1,1)=',' THEN substring(a.passengerSId,2) ELSE a.passengerSId END), ',')) a as passengerDepId ")
            .append(" where a.rank>= ").append(Long.toString((batchNum - 1) * pageSize + 1)).append(" and a.rank<= ").append(Long.toString(batchNum * pageSize))
            .append(" and a.REFUND_STATUS IN ('02','03') and a.validTime between '" + args[0] + "' and '" + args[1] + "'" + ") a");
        this.jdbcTemplate.execute(querySql.toString());
    }

    /**
     * 执行HIVE
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    private List<Map<String, Object>> executeQuerySQL(String sql, String... args) throws SQLException {
        try {
            return executeSQL(sql, args);
        } catch (SQLException e) {
            logger.error("<reportcollectionapi><HiveDAOImpl><executeQuerySQL><executeQuerySQL><executeQuerySQL>" + "【执行sql】执行sql，入参=" + JSON.toJSONString(args)
                         + ",异常********************************" + org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e));
            throw e;
        }

    }
}
