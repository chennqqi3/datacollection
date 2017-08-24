package com.ly.report.tmc.datacollectionapi.biz.common;

/**
 * Created by hyz46086 on 2017/4/17.
 */
public class Constant {

    //    public static final String  MQ_ADDRESS = "10.100.159.200:9876;10.100.157.34:9876";
    public static final String MQ_ADDRESS            = "10.100.191.156:9876;10.100.191.160:9876";
    //    public static final String  MQ_ADDRESS = "172.16.9.171:9876";

    //    public static final String  ZK_ADDRESS = "10.100.156.42:2181,10.100.156.43:2181,10.100.156.44:2181";
    //    public static final String  ZK_ADDRESS = "172.18.63.23:2181,172.18.63.24:2181,172.18.63.25:2181";
    //    public static final String  ZK_ADDRESS = "172.16.62.2:2181,172.16.62.3:2181,***.**.**.*:2181,172.16.62.5:2181,172.16.62.6:2181";
    public static final String ZK_ADDRESS            = "172.16.140.193:2181,172.16.140.194:2181,172.16.140.195:2181,172.16.140.196:2181";

    public static final String ZK_NAMESPACE          = "TCTMCstatisticsNode";

    public static final String MESSAGE_GROUP         = "create_topic_group_prod";
    public static final String MESSAGE_GROUP_UPDATE  = "Batch_Num_Group";

    public static final String CHANGE_MESSAGE_GROUP  = "change_topic_group";
    public static final String REFUND_MESSAGE_GROUP  = "refund_topic_group";

    public static final String ASSIGN_TOPIC          = "assign_topic";
    public static final String CREATE_TOPIC          = "create_topic";
    public static final String PRIMITIVE_TOPIC_BATCH = "primitive_batch";
    public static final String CHANGE_TOPIC          = "change_topic";
    public static final String CHANGE_TOPIC_BATCH    = "change_batch";
    public static final String REFUND_TOPIC          = "refund_topic";
    public static final String REFUND_TOPIC_BATCH    = "refund_batch";
    public static final int    VALID_TIME            = 20000;

    //    public static final String  DATABASE = "tctmcorder";
}
