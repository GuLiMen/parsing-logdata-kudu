package cn.qtech.bigdata.comm;

import java.util.*;

import static java.util.Arrays.asList;

public class AppConstants {
    // AA机Log写入phoenix库表
    public static final String AALOG_SINK_IMPALA_SCHEMA = "ods_product_process";
    public static final String AALOG_SINK_IMPALA_TABLE_TH = "aa_reject_th_test";
    public static final String AALOG_SINK_IMPALA_TABLE_CB = "aa_reject_th_test";
    public static final String AALOG_SINK_IMPALA_TABLE_GC = "aa_reject_th_test";

    public static final String PHOENIX_DUTY_DATABASE = "ODS_BASE";
    public static final String PHOENIX_DUTY_TABLE = "DEVICE_INFO";

    // phoenix
    //public static final String PHOENIX_URL = "jdbc:phoenix:10.170.6.133,10.170.6.134,10.170.6.135:2181";
    public static final String PHOENIX_URL = "jdbc:phoenix:bigdata01,bigdata02,bigdata03:2181";
    //impala
    public static final String  IMPALA_URL = "jdbc:impala://10.170.3.12:21050/ods_product_process;UseSasl=0;AuthMech=3;UID=qtkj;PWD=";

    //AA_LIST
    public static final String SENSOR_THROW_TIGGER = "sensor_throw_trigger";
    public static final String INIT_FAIL_CNT = "INIT_FAIL_CNT";
    public static final String SID_CNT = "SID_CNT";
    public static final String REINIT_FAIL_CNT = "REINIT_FAIL_CNT";
    public static final String AA3_FAIL_CNT = "AA3_FAIL_CNT";
    public static final String Y_level_FAIL_CNT = "Y_LEVEL_FAIL_CNT";
    public static final String MTF_CHECK2_FAIL_CNT = "MTF_CHECK2_FAIL_CNT";
    public static final String CHART_ALIGNMENT2_FAIL_CNT = "CHART_ALIGNMENT2_FAIL_CNT";
    public static final String EPOXY_INSPECTION_FAIL_CNT = "EPOXY_INSPECTION_FAIL_CNT";
    public static final String SENSORID_CHECK_FAIL_CNT = "SENSORID_CHECK_FAIL_CNT";
    public static final String VCM_INIT_FAIL_CNT = "VCM_INIT_FAIL_CNT";
    public static final String VCM_INIT2_FAIL_CNT = "VCM_INIT2_FAIL_CNT";
    public static final String VCM_INIT3_FAIL_CNT = "VCM_INIT3_FAIL_CNT";
    public static final String VCM_POWERON_FAIL_CNT = "VCM_POWERON_FAIL_CNT";
    public static final String VCM_POWERON2_FAIL_CNT = "VCM_POWERON2_FAIL_CNT";
    public static final String VCM_POWERON3_FAIL_CNT = "VCM_POWERON3_FAIL_CNT";
    public static final String VCM_HALL_FAIL_CNT = "VCM_HALL_FAIL_CNT";
    public static final String VCM_HALL2_FAIL_CNT = "VCM_HALL2_FAIL_CNT";
    public static final String VCM_MOVE_FAIL_CNT = "VCM_MOVE_FAIL_CNT";
    public static final String VCM_MOVE2_FAIL_CNT = "VCM_MOVE2_FAIL_CNT";
    public static final String INIT_CHECK_FAIL_CNT = "INIT_CHECK_FAIL_CNT";
    public static final String BLEMISH_DEFECT_FAIL_CNT = "BLEMISH_DEFECT_FAIL_CNT";
    public static final String LIGHTPANEL_OC_FAIL_CNT = "LIGHTPANEL_OC_FAIL_CNT";
    public static final String UV_MTF_CHECK2_FAIL_CNT = "UV_MTF_CHECK2_FAIL_CNT";
    public static final String SAVEOC_FAIL_CNT = "SAVEOC_FAIL_CNT";
    public static final String GRIPPEROPEN_CNT = "GRIPPEROPEN_CNT";
    public static final String VCM_OIS_INIT_FAIL_CNT = "VCM_OIS_INIT_FAIL_CNT";
    public static final String VCM_OIS_INIT2_FAIL_CNT = "VCM_OIS_INIT2_FAIL_CNT";

    //mysql
 /*   public static final String MYSQL_HOST ="10.170.6.160";
    public static final String MYSQL_PORT ="3306";
    public static final String MYSQL_USER ="ziyunIot";
    public static final String MYSQL_PASSWD ="Pass1234";
    public static final String MYSQL_DATABASE ="ziyun-iot";
    public static final String MYSQL_TABLE ="t_device_calcgd1jh1u82gwwionk";*/

    //sql output path
    public static final String BACKFILE_ROOTPATH = "D:\\aaupsert\\AANotReadLogbak";

    public static final String RESULT_OUTPUT_LOCAL_DIR = "D:\\aaupsert\\AANotReadLogsql";
    public static final String HDFS_FLUME_DIR = "/tmp/flume";

    // 插入失败
    public static final String PHOENIX_UPSERT_ERROR_CODE = "001001";
    public final static String PHOENIX_UPSERT_ERROR_MESSAGE = "插入语句失败!!";

    public final static String PHOENIX_ALTER_ERROR_CODE = "001002";
    public final static String PHOENIX_ALETR_ERROR_MESSAGE = "建表 或 修改表结构失败!!";

    public final static String PHOENIX_SELECT_ERROR_CODE = "001003";
    public final static String PHOENIX_SELECT_ERROR_MESSAGE = "查询失败 !!";

    /**
     * job失败接收邮箱
     */
    public final static List<String> RECEIVE_EMAIL = asList("limeng.gu@qtechglobal.com", "liqin.liu@qtechglobal.com", "wenliang.li_it@qtechglobal.com");


}