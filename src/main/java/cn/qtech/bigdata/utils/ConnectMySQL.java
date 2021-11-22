package cn.qtech.bigdata.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ConnectMySQL {

    /**
     * 获取 MySQL 连接
     *
     * @param url
     * @return
     */
    public static Connection  getConnect(String url,String user,String posswd){
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection(url,user,posswd);


        } catch (Exception e) {
            e.printStackTrace();
        }
        return con;
    }

    /**
     * 关闭连接
     *
     * @param conn
     */
    public static void closeConn( Connection conn){
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
