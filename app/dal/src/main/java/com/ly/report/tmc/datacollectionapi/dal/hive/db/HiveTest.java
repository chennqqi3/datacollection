package com.ly.report.tmc.datacollectionapi.dal.hive.db;

import com.ly.report.tmc.datacollectionapi.dal.common.DalConstant;
import com.ly.report.tmc.datacollectionapi.dal.hive.db.to.OrderPassengerTick;
import com.ly.report.tmc.datacollectionapi.dal.hive.db.to.OrderPassengerTickUpdate;
import com.ly.report.tmc.datacollectionapi.dal.util.UUIDBuildDal;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.RowMapperResultSetExtractor;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import  org.apache.hadoop.hdfs.DFSClient;

/**
 * Created by hyz46086 on 2017/4/19.
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:META-INF/spring/*.xml" })
public class HiveTest {

    @Resource
    private HiveDAO hiveDAO;

    @Resource
    JdbcTemplate jdbcTemplate;


    @Test
    public void batchInsertOrderDep() {

        List<Object[]> args = new ArrayList<>();
        Object[] objects = new Object[]{"8a8194bc5bcd282f015bcd282f430001",1,"DA20161130203445773",929,"0","1483436948000",1,0,1,1};
//        String sql ="insert  into department_order_info value (?,?','?','?','?','?','?','?','?','?')";
        String sql ="insert  into department_order_info values (?,?,?,?,?,?,?,?,?,?)";
        args.add(objects);
        int[] ints = jdbcTemplate.batchUpdate(sql,args);
        System.out.print(ints);

    }


    @Test
    public void createTable(){

//        String sql = " CREATE TABLE PASSENGER( ID bigint," +
//                "    PASSENGER_NAME   varchar(255)," +
//                "    PASSENGER_ENLISH_NAME   varchar(255),\n" +
//                "    PASSENGER_NICKNAME   varchar(255),\n" +
//                "    PASSENGER_COMPANY_ID   bigint,\n" +
//                "    PASSENGER_COMPANY   varchar(255),\n" +
//                "    PASSENGER_COST_CENTER_NAME   varchar(64),\n" +
//                "    PASSENGER_COST_CENTER_ID   bigint,\n" +
//                "    PASSENGER_COST_CENTER_ENNAME   varchar(64),\n" +
//                "    PASSENGER_DEPARTMENT_ID   bigint ,\n" +
//                "    PASSENGER_DEPARTMENT_NAME   varchar(255),\n" +
//                "    PASSENGER_STRUCTURE    varchar(255) ,\n" +
//                "    PASSENGER_STRUCTURE_ID  varchar(255) ,\n" +
//                "    PASSENGER_SEX   varchar(1),\n" +
//                "    PASSENGER_PHONE   varchar(64) ,\n" +
//                "    PASSENGER_EMAIL   varchar(255),\n" +
//                "    PASSENGER_BIRTH_DATE   timestamp,\n" +
//                "    PASSENGER_EMPLOYEE_ID   bigint,\n" +
//                "    PASSENGER_NATIONALITY   varchar(64),\n" +
//                "    PASSENGER_NO   varchar(64),\n" +
//                "    PASSENGER_TYPE   varchar(4) ,\n" +
//                "    PASSENGER_CLASS   varchar(4),\n" +
//                "  IS_VIP int,\n" +
//                "  PASSENGER_TEXT varchar(255) ,\n" +
//                "  ITINERARY_NO varchar(64),\n" +
//                "  GMT_CREATE timestamp ,\n" +
//                "  GMT_MODIFIED timestamp,\n" +
//                "  OPERATOR varchar(255))";

        String sql2 = "DROP TABLE IF EXISTS "+ DalConstant.DATABASE+".DEPARTMENT_ORDER_INFO_partition";
//        String sql2 = "DROP TABLE IF EXISTS FLIGHT_SEGMENT ";

//        String sql3 =  "ALTER TABLE "+ DalConstant.DATABASE+".DEPARTMENT_ORDER_INFO_partition DROP IF EXISTS PARTITION(dateTime='2017-01-05')";



//        String create = "CREATE TABLE IF NOT EXISTS DEPARTMENT_ORDER_INFO_create(ID varchar(255),DEPARTMENT_ID BIGINT,ORDER_ID varchar(255), PASSENGER_ID BIGINT,PASSENGER_TYPE varchar(255) ,validTime string,TMC_ID BIGINT,COMPANY_ID BIGINT,ORDER_STATUS int ,BLOODRELATION int,itemId BIGINT,orderCreateTime string, topic string,messageId String )";
//        String refund = "CREATE TABLE IF NOT EXISTS DEPARTMENT_ORDER_INFO_refund(ID varchar(255),DEPARTMENT_ID BIGINT,ORDER_ID varchar(255), PASSENGER_ID BIGINT,PASSENGER_TYPE varchar(255) ,validTime string,TMC_ID BIGINT,COMPANY_ID BIGINT,ORDER_STATUS int ,BLOODRELATION int,itemId BIGINT,orderCreateTime string, topic string )";
//        String change = "CREATE TABLE IF NOT EXISTS DEPARTMENT_ORDER_INFO_change(ID varchar(255),DEPARTMENT_ID BIGINT,ORDER_ID varchar(255), PASSENGER_ID BIGINT,PASSENGER_TYPE varchar(255) ,validTime string,TMC_ID BIGINT,COMPANY_ID BIGINT,ORDER_STATUS int ,BLOODRELATION int,itemId BIGINT,orderCreateTime string, topic string )";
//        String sb2 = "CREATE TABLE "+ DalConstant.DATABASE+".DEPARTMENT_ORDER_INFO_partition (ID varchar(255),DEPARTMENT_ID BIGINT,ORDER_ID varchar(255), PASSENGER_ID BIGINT,PASSENGER_TYPE varchar(255),validTime string,TMC_ID BIGINT,COMPANY_ID BIGINT,BLOODRELATION int,itemId BIGINT,orderCreateTime string, topic string,messageId String) partitioned by (dateTime string,order_status int) row format delimited fields terminated by '\t'";
        String sb1 = "CREATE TABLE "+ DalConstant.DATABASE+".DEPARTMENT_ORDER_INFO_partition (DEPARTMENT_ID BIGINT,ORDER_ID varchar(255), PASSENGER_ID BIGINT,PASSENGER_TYPE varchar(255),validTime string,TMC_ID BIGINT,COMPANY_ID BIGINT,ORDER_STATUS int,BLOODRELATION int,itemId BIGINT,orderCreateTime string) partitioned by (dateTime string) row format delimited fields terminated by '\t'";
//        String sql = "CREATE TABLE department_order_info(ID varchar(255),PASSENGER_NAME varchar(255),PASSENGER_ID BIGINT,PASSENGER_TYPE varchar(255) ,CREATETIME timestamp  ,TMC_ID BIGINT,COMPANY_ID BIGINT,ORDER_STATUS int ,BLOODRELATION int)";
//        String sql = "select DISTINCT foi.PASSENGER_ID as passengerId, pa.PASSENGER_DEPARTMENT_ID as passengerDepId ,pa.passenger_structure_id as passengerSId ,pa.PASSENGER_COMPANY_ID as companyId ,foi.FLIGHT_ORDER_NO  as orderNum ,pa.PASSENGER_TYPE as passengerType  from FLIGHT_ORDER_ITEM as foi  LEFT JOIN PASSENGER as pa  on  foi.PASSENGER_ID = pa.ID  LEFT JOIN FLIGHT_TICKET as ft ON foi.FLIGHT_TICKET_ID = ft.ID where foi.FLIGHT_ORDER_NO = 'DA20161130203445773' and ft.TICKET_TYPE=1";
        hiveDAO.createTables(sql2);
        hiveDAO.createTables(sb1);
//        hiveDAO.createTables(refund);
//        hiveDAO.createTables(change);
//        try {
//            List<Map<String, Object>> objectList = hiveDAO.executeQuerySQL("");
//            System.out.print(objectList);
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        List<OrderPassengerTick> orderPassengerTicks =jdbcTemplate.query(sql,new String[]{"DA20161130203445773"}, new RowMapper() {
//            @Override
//            public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
//                return null;
//            }
//        });

//        System.out.print(orderPassengerTicks);
    }


    @Test
    public void findTest(){
//        String sql = "select * from tctmcorder.DEPARTMENT_ORDER_INFO_partition ";
        String sql = "select * from tctmcorder.Temporary_20170102 ";

        try {
            List<Map<String, Object>> objectList = hiveDAO.executeQuerySQL(sql);
            System.out.print(objectList);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void findonly(){
            String sql  ="select DISTINCT foi.PASSENGER_ID as passengerId, foi.ID as itemId,pa.PASSENGER_DEPARTMENT_ID as passengerDepId ,pa.PASSENGER_STRUCTURE_ID as passengerSId ,pa.PASSENGER_COMPANY_ID as companyId ,foi.FLIGHT_ORDER_NO  as orderNum ,pa.PASSENGER_CLASS as passengerType  from FLIGHT_ORDER_ITEM as foi  LEFT JOIN PASSENGER as pa  on  foi.PASSENGER_ID = pa.ID  LEFT JOIN FLIGHT_TICKET as ft ON foi.FLIGHT_TICKET_ID = ft.ID where foi.FLIGHT_ORDER_NO = ? and ft.TICKET_TYPE=1 and foi.ITEM_TYPE=0";
//        String sql = "SELECT pa.ID as passengerId, pa.PASSENGER_DEPARTMENT_ID as passengerDepId ,pa.PASSENGER_STRUCTURE_ID as passengersId ,pa.PASSENGER_COMPANY_ID as companyId,foi.FLIGHT_ORDER_NO as orderNum ,pa.PASSENGER_TYPE as passengerType from FLIGHT_REFUND_APPLY_ITEM as frai LEFT JOIN FLIGHT_REFUND_APPLY as fra ON frai.FLIGHT_REFUND_APPLY_NO = fra.APPLY_NO LEFT JOIN FLIGHT_ORDER_ITEM as foi ON frai.ORDER_ITEM_ID=foi.ID LEFT JOIN PASSENGER as pa on foi.PASSENGER_ID=pa.ID where fra.APPLY_NO=?";
//        String sql  ="select DISTINCT foi.PASSENGER_ID as passengerId, foi.ID as itemId,pa.PASSENGER_DEPARTMENT_ID as passengerDepId ,pa.PASSENGER_STRUCTURE_ID as passengerSId ,pa.PASSENGER_COMPANY_ID as companyId ,foi.FLIGHT_ORDER_NO  as orderNum ,pa.PASSENGER_CLASS as passengerType  from FLIGHT_ORDER_ITEM as foi  LEFT JOIN PASSENGER as pa  on  foi.PASSENGER_ID = pa.ID  LEFT JOIN FLIGHT_TICKET as ft ON foi.FLIGHT_TICKET_ID = ft.ID where foi.FLIGHT_ORDER_NO = ? and ft.TICKET_TYPE=1 and foi.ITEM_TYPE=0";
        List<OrderPassengerTick> orderPassengerTicks =jdbcTemplate.query(sql,new Object[]{"DA20161214151042146"}, new RowMapper() {
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
        System.out.print(orderPassengerTicks);
    }

        @Test
        public void findby(){

            List<Object[]> args = new ArrayList<>();
            args.add(new Object[]{"8a8194bc5bebb0a7015bebb0a7d20001","0","DA20161220210851341","992"," ","2016-12-29 14:07:54.0","1","1",0,1,"125513"," ","topic"});
            String sql ="insert  into DEPARTMENT_ORDER_INFO_create  values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
            jdbcTemplate.batchUpdate(sql,args);
//            jdbcTemplate.execute(sql);
        }


        @Test
        public void insertTable(){
            List<Object[]> argsList = new ArrayList<>();
            ArrayList<Object> objects = new ArrayList<>();
            objects.add("test1234");
            objects.add("1");
            objects.add("test1234");
            objects.add("1");
            objects.add("0");
            objects.add("2012-02-03 :00:00:00");
            objects.add(1);
            objects.add(1);
            objects.add("0");
            objects.add("1");
            objects.add("1");
            objects.add("2012-02-03 :00:00:00");
            objects.add("topic");
            objects.add("test123");
            objects.add("2013-02-03");
            argsList.add(objects.toArray());
            hiveDAO.dropTable("test123");
            hiveDAO.createTable("test123");
            hiveDAO.batchInsertOrderDep(argsList,"test123");
            hiveDAO.batchInsertOrderByPartition(new Object[]{"'2013-02-03'",2,"test123"});
            System.out.print("123");
        }


        @Test
        public void findByPage(){


            String sql  = "SELECT a.GMT_CREATE as createTime,a.ORDER_NO as codeId,a.OUT_TICKET_TIME as finishTime, a.TMC_ID as tmcId  FROM (SELECT *,row_number () over ( ORDER BY id DESC ) rank FROM FLIGHT_ORDER)a where a.rank>= 1 and a.rank<= 5000 and OUT_TICKET_TIME between ? and ?";

            try {
                hiveDAO.executeSQL(sql, new String[]{"2016-01-01 00:00:00","2017-01-01 23:59:59"});
                System.out.print("123");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }



        @Test
        public void testConnect(){

            System.out.println("get apps...");
            Connection conn = null;
            try {
                Class.forName("org.apache.hive.jdbc.HiveDriver");
                conn = DriverManager.getConnection("jdbc:hive2://10.100.156.93:10000/tctmcorder", "flightdap", "Ss3yRt#C");
                String sql = "select * from DEPARTMENT_ORDER_INFO_partition ";

                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.execute(sql);

            } catch (Exception e) {
                e.printStackTrace();
            }

        }


    @Test
    public void testFindResult(){
        hiveDAO.dropTable("Temporary_20170102");
//        hiveDAO.dropTable("business_travel_policy");
        hiveDAO.createTable("Temporary_20170102");
//        String sql = "select *  FROM (select foi.PASSENGER_ID as passengerId, fo.GMT_CREATE as createTime,fo.ORDER_NO as codeId,fo.OUT_TICKET_TIME as finishTime,fo.TMC_ID as tmcId ,foi.ID as itemId,pa.PASSENGER_DEPARTMENT_ID as passengerDepId ,pa.PASSENGER_STRUCTURE_ID as passengerSId ,pa.PASSENGER_COMPANY_ID as companyId ,foi.FLIGHT_ORDER_NO  as orderNum ,pa.PASSENGER_CLASS as passengerType ,foi.ITEM_TYPE as item_type ,row_number () over ( ORDER BY fo.ORDER_NO DESC ) rank FROM  tctmcorder.FLIGHT_ORDER_ITEM as foi  LEFT JOIN tctmcorder.PASSENGER as pa  on  foi.PASSENGER_ID = pa.ID LEFT JOIN tctmcorder.FLIGHT_ORDER as fo on foi.FLIGHT_ORDER_NO = fo.ORDER_NO LEFT JOIN  tctmcorder.FLIGHT_TICKET as ft ON foi.FLIGHT_TICKET_ID = ft.ID) as a   where a.rank>= 1 and a.rank<= 5000 and a.item_type=0 and a.finishTime between '2017-01-01 00:00:00' and '2017-12-01 23:59:59'";
//        String sql = " CREATE TABLE IF NOT EXISTS tctmcorder.Temporary_20170101 row format delimited fields terminated by '\t' " +
        String sql = " insert into tctmcorder.Temporary_20170102 select a.passengerSId, a.passengerDepId,a.ORDER_ID, a.PASSENGER_ID, a.PASSENGER_TYPE,a.validTime,a.tmcId ,a.ORDER_STATUS,(CASE WHEN passengerDepId =  TOP_ID THEN 1 ELSE 0 END) BLOODRELATION,a.itemId,a.orderCreateTime,a.datetime,a.companyId "
                + " FROM (select  foi.FLIGHT_ORDER_NO as ORDER_ID,foi.PASSENGER_ID as PASSENGER_ID, pa.PASSENGER_CLASS as PASSENGER_TYPE ,ft.TICKET_STATUS as TICKET_STATUS, fo.OUT_TICKET_TIME as validTime, fo.TMC_ID as tmcId ,0 ORDER_STATUS,"
                + "(CASE WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 0 THEN PASSENGER_STRUCTURE_ID WHEN LENGTH(PASSENGER_STRUCTURE_ID)-LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 1 AND substring(PASSENGER_STRUCTURE_ID,1,1)=',' THEN substring(PASSENGER_STRUCTURE_ID,2) WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) = 1 AND substring(PASSENGER_STRUCTURE_ID,1,1)<>','  THEN regexp_extract(PASSENGER_STRUCTURE_ID, '(.*?)(,)(.*?)', 1) WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) > 1 AND substring(PASSENGER_STRUCTURE_ID,1,1) =','  THEN regexp_extract(PASSENGER_STRUCTURE_ID, ',(.*?)(,)(.*?)', 1) WHEN LENGTH(PASSENGER_STRUCTURE_ID) - LENGTH(regexp_replace(PASSENGER_STRUCTURE_ID,',','')) > 1 AND substring(PASSENGER_STRUCTURE_ID,1,1) <>',' THEN regexp_extract(PASSENGER_STRUCTURE_ID, '(.*?)(,)(.*?)', 1) END) TOP_ID"
                + ",foi.ID as itemId,fo.GMT_CREATE as orderCreateTime,'2017-01-02' datetime,(CASE WHEN PASSENGER_STRUCTURE_ID ='' THEN PASSENGER_DEPARTMENT_ID ELSE PASSENGER_STRUCTURE_ID END ) as passengerSId ,pa.PASSENGER_COMPANY_ID as companyId ,foi.ITEM_TYPE as item_type,row_number () over ( ORDER BY fo.ORDER_NO DESC ) rank FROM  tctmcorder.FLIGHT_ORDER_ITEM as foi  LEFT JOIN tctmcorder.PASSENGER as pa  on  foi.PASSENGER_ID = pa.ID LEFT JOIN tctmcorder.FLIGHT_ORDER as fo on foi.FLIGHT_ORDER_NO = fo.ORDER_NO LEFT JOIN tctmcorder.FLIGHT_TICKET as ft ON foi.FLIGHT_TICKET_ID = ft.ID) as a lateral view explode(split((CASE WHEN substring(a.passengerSId ,1,1)=',' THEN substring(a.passengerSId,2) ELSE a.passengerSId END), ',')) a as passengerDepId where a.rank>= 1 and a.rank<= 5000 and a.item_type=0 and a.validTime between '2017-01-02 00:00:00' and '2017-12-01 23:59:59'";;
       hiveDAO.createTables(sql);
    }

}
