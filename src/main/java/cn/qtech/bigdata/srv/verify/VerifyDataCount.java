package cn.qtech.bigdata.srv.verify;

import cn.qtech.bigdata.model.bufferStream.ReadLocalModel;
import cn.qtech.bigdata.model.bufferStream.WriteLocalModel;
import cn.qtech.bigdata.utils.CloseStream;
import cn.qtech.bigdata.utils.ConnectPhoenixDB;
import cn.qtech.bigdata.utils.OperateFie;
import cn.qtech.bigdata.utils.PhoenixJDBC;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import static cn.qtech.bigdata.comm.AppConstants.PHOENIX_URL;

public class VerifyDataCount {

    private static String table;

    public static void main(String[] args) throws IOException {
        System.out.println("=============VerifyDataCount===========");
        table = System.getProperty("table");
        verifyRowkey();

    }


    private static void verifyRowkey() throws IOException {
        OperateFie operateFie = new OperateFie();
        ReadLocalModel verifyModel = operateFie.readLocalFile("/data/workspace/alarm/verifyDataCount/verifyTempData");
        WriteLocalModel sidMainAAStartRepeatModel = operateFie.writeLocalFile("/data/workspace/alarm/verifyDataCount/sidMainAAStartRepeat", true);
        WriteLocalModel normalModel = operateFie.writeLocalFile("/data/workspace/alarm/verifyDataCount/noraml", true);
        WriteLocalModel rowkeyContainsRD = operateFie.writeLocalFile("/data/workspace/alarm/verifyDataCount/rowkeyContainsRD", true);

        String line;
        Connection connection = ConnectPhoenixDB.getConnection(PHOENIX_URL);
        PhoenixJDBC phoenixJDBC = new PhoenixJDBC();
        while ((line = verifyModel.getBufferedReader().readLine()) != null) {

            String[] lineArr = line.split(",");
            String MAIN_AA_START = lineArr.length > 2 ? lineArr[2] : "";

            if (lineArr.length < 2) {
                System.out.println(line + " length != 2");
                continue;
            }
            if (lineArr[0].contains("rd")) {
                rowkeyContainsRD.getBufferedWriter().write(lineArr[0] + ",MAIN_AA_START=" + MAIN_AA_START);
                rowkeyContainsRD.getBufferedWriter().newLine();
                rowkeyContainsRD.getBufferedWriter().flush();
                continue;
            }

            // lineArr[0]:ROWKEY;lineArr[1]:file
            String sql = "select FILE from ODS_PRODUCT_PROCESS." + table + " where ROWKEY='" + lineArr[0] + "'";
            ResultSet resultSet = phoenixJDBC.selectResult(connection, sql);
            String file = "";
            try {
                while (resultSet.next()) {
                    file = resultSet.getString("FILE");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            if (!lineArr[1].equals(file)) {

                sidMainAAStartRepeatModel.getBufferedWriter().write(lineArr[1] + "!=" + file + ",ROWKEY=" + lineArr[0] + ",MAIN_AA_START=" + MAIN_AA_START);
                sidMainAAStartRepeatModel.getBufferedWriter().newLine();
                sidMainAAStartRepeatModel.getBufferedWriter().flush();
            } else {
                normalModel.getBufferedWriter().write(lineArr[1] + "=" + file + ",ROWKEY=" + lineArr[0]);
                normalModel.getBufferedWriter().newLine();
                normalModel.getBufferedWriter().flush();
            }

        }
        ConnectPhoenixDB.closeConn(connection);
        CloseStream.closeReadLocal(verifyModel);
        CloseStream.closeWriteLocal(sidMainAAStartRepeatModel);
        CloseStream.closeWriteLocal(normalModel);
        CloseStream.closeWriteLocal(rowkeyContainsRD);


    }

}
