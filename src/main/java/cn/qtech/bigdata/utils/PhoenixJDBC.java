package cn.qtech.bigdata.utils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import cn.qtech.bigdata.comm.SendEMailWarning;
import cn.qtech.bigdata.model.AlterTbleModel;
import cn.qtech.bigdata.model.bufferStream.WriteLocalModel;
import org.apache.commons.lang.ArrayUtils;

import static cn.qtech.bigdata.comm.AppConstants.RECEIVE_EMAIL;


public class PhoenixJDBC {


    /**
     * 根据 SQL 插入数据
     *
     * @param sql
     */
    public void upsertData(Connection conn, String sql) {
        try {
            Statement statement = conn.createStatement();
            statement.executeUpdate(sql);
            conn.commit();
        } catch (Exception e) {
            System.out.println(e.getMessage() + "\r\n失败SQL:" + sql);
            System.out.println("ERROR!!========== \n ${PHOENIX_UPSERT_ERROR_CODE}   ${PHOENIX_UPSERT_ERROR_MESSAGE} ");
            SendEMailWarning.sendMail(RECEIVE_EMAIL, this.getClass().getName() + ".upsertData() Job ERROR", this.getClass().getName() + e.getStackTrace() + " \r\n " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));

        }
    }


    /**
     * 根据 SQL 建表/添加字段
     *
     * @param sql
     */
    public void creOrAlterTable(Connection conn, String sql) {
        try {
            Statement statement = conn.createStatement();
            statement.executeUpdate(sql);
            conn.commit();
        } catch (Exception e) {
            System.out.println(e.getMessage() + "\r\n失败SQL:" + sql);
            System.out.println("ERROR!!========== \n ${PHOENIX_UPSERT_ERROR_CODE}   ${PHOENIX_UPSERT_ERROR_MESSAGE} ");
            SendEMailWarning.sendMail(RECEIVE_EMAIL, this.getClass().getName() + ".creOrAlterTable() Job ERROR", this.getClass().getName() + e.getStackTrace() + " \r\n " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));

        }
    }

    /**
     * 根据 對象 添加字段
     *
     * @param
     */
    public void creOrAlterTable(Connection conn, AlterTbleModel alterTbleModel) throws IOException {
        try {

            Statement statement = conn.createStatement();
            statement.executeUpdate(alterTbleModel.getCBsql());
            conn.commit();
            statement.executeUpdate(alterTbleModel.getGCsql());
            conn.commit();
            statement.executeUpdate(alterTbleModel.getTHsql());
            conn.commit();

        } catch (Exception e) {
            WriteLocalModel alterTableError = new OperateFie().writeLocalFile("/data/workspace/alarm/alterTableError", true);
            alterTableError.getBufferedWriter().write(alterTbleModel.getCBsql());
            alterTableError.getBufferedWriter().newLine();
            alterTableError.getBufferedWriter().write(alterTbleModel.getGCsql());
            alterTableError.getBufferedWriter().newLine();
            alterTableError.getBufferedWriter().write(alterTbleModel.getTHsql());
            alterTableError.getBufferedWriter().newLine();
            alterTableError.getBufferedWriter().flush();
            CloseStream.closeWriteLocal(alterTableError);
            if (alterTbleModel.getNotExitsCloumn() != null) {
                System.out.println("array=====" + ArrayUtils.toString(alterTbleModel.getNotExitsCloumn().toArray()));
                splitAlterTable(conn, alterTbleModel.getNotExitsCloumn());
            }

        }

    }

    public void splitAlterTable(Connection conn, ArrayList<String> notExitsCloumn) throws IOException {

        String THalterSQLPrefix = "ALTER TABLE $table_TH ADD if not exists ";
        String CBalterSQLPrefix = "ALTER TABLE $table_GC ADD if not exists ";
        String GCalterSQLPrefix = "ALTER TABLE $table_CB ADD if not exists ";
        Statement statement = null;
        try {
            statement = conn.createStatement();
        } catch (Exception e) {
            System.out.println(e.getStackTrace() + e.getMessage());
            SendEMailWarning.sendMail(RECEIVE_EMAIL, this.getClass().getName() + ".splitAlterTable() Job ERROR", this.getClass().getName() + e.getStackTrace() + " \r\n " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));

        }
        if (statement == null) {
            SendEMailWarning.sendMail(RECEIVE_EMAIL, this.getClass().getName() + ".splitAlterTable() Job ERROR \r\n ", "原因:statement == null" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
            return;
        }
        for (String col : notExitsCloumn) {
            String THalterSQL = THalterSQLPrefix + col + " varchar";
            String CBalterSQL = CBalterSQLPrefix + col + " varchar";
            String GCalterSQL = GCalterSQLPrefix + col + " varchar";
            try {
                statement.executeUpdate(THalterSQL);
                statement.executeUpdate(CBalterSQL);
                statement.executeUpdate(GCalterSQL);
                conn.commit();
            } catch (Exception e) {

                WriteLocalModel addColumnError = new OperateFie().writeLocalFile("/data/workspace/alarm/addSingleColumnError", true);
                addColumnError.getBufferedWriter().write(THalterSQL);
                addColumnError.getBufferedWriter().newLine();
                addColumnError.getBufferedWriter().write(CBalterSQL);
                addColumnError.getBufferedWriter().newLine();
                addColumnError.getBufferedWriter().write(GCalterSQL);
                addColumnError.getBufferedWriter().newLine();
                addColumnError.getBufferedWriter().flush();
                CloseStream.closeWriteLocal(addColumnError);

            }

        }
    }


    /**
     * 返回查询结果
     *
     * @param sql
     * @return
     */
    public ResultSet selectResult(Connection conn, String sql) {
        ResultSet rs = null;
        try {
            Statement statement = conn.createStatement();
            rs = statement.executeQuery(sql);
        } catch (Exception e) {

            System.out.println(e.getMessage());
            System.out.println("ERROR!!========== \n ${PHOENIX_SELECT_ERROR_CODE}   ${PHOENIX_SELECT_ERROR_MESSAGE} ");
            SendEMailWarning.sendMail(RECEIVE_EMAIL, this.getClass().getName() + ".selectResult() Job ERROR", this.getClass().getName() + e.getStackTrace() + " \r\n " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));

        }
        return rs;
    }

    /**
     * 遍历结果集
     *
     * @param rs
     * @return
     */
    public List<String> foreachResultSet(ResultSet rs) {

        ArrayList<String> colList = new ArrayList<>();
        try {
            while (rs.next()) {
                colList.add(rs.getString("COLUMN_NAME"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            SendEMailWarning.sendMail(RECEIVE_EMAIL, this.getClass().getName() + ".foreachResultSet() Job ERROR", this.getClass().getName() + e.getStackTrace() + " \r\n " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));

        }
        return colList;
    }



}
