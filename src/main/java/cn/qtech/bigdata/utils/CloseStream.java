package cn.qtech.bigdata.utils;

import cn.qtech.bigdata.comm.SendEMailWarning;
import cn.qtech.bigdata.model.bufferStream.ReadHDFSModel;
import cn.qtech.bigdata.model.bufferStream.ReadLocalModel;
import cn.qtech.bigdata.model.bufferStream.WriteHDFSModel;
import cn.qtech.bigdata.model.bufferStream.WriteLocalModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static cn.qtech.bigdata.comm.AppConstants.RECEIVE_EMAIL;

public class CloseStream {
    private static final Logger LOG = LoggerFactory.getLogger(CloseStream.class);

    public static void closeWriteLocal(WriteLocalModel writeLocalModel) {
        if (writeLocalModel == null) {
            LOG.warn("writeLocalModel == null");
            SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.utils.CloseStream.closeWriteLocal" ,  "cn.qtech.bigdata.utils.CloseStream.closeWriteLocal() Job warn \r\n writeLocalModel=null"+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) );

            return;
        }
        try {
            writeLocalModel.getBufferedWriter().close();
            writeLocalModel.getOutputStreamWriter().close();
            writeLocalModel.getFileOutputStream().close();
        } catch (IOException e) {
            LOG.error("关闭写本地文件异常!!");
            SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.utils.CloseStream.closeWriteLocal" ,  "cn.qtech.bigdata.utils.CloseStream.closeWriteLocal() Job ERROR 关闭流异常!! \r\n " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + e.getMessage() + e.getStackTrace());

            e.printStackTrace();
        }

    }

    public static void closeReadLocal(ReadLocalModel readLocalModel) {
        if (readLocalModel == null) {
            LOG.warn("readLocalModel == null");
            SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.utils.CloseStream.closeReadLocal" ,  "cn.qtech.bigdata.utils.CloseStream.closeReadLocal() Job warn \r\n readLocalModel=null"+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) );
            return;
        }
        try {
            readLocalModel.getBufferedReader().close();
            readLocalModel.getInputStreamReader().close();
            readLocalModel.getFileInputStream().close();

        } catch (IOException e) {
            LOG.error("关闭读本地文件异常!!");
            SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.utils.CloseStream.closeReadLocal" ,  "cn.qtech.bigdata.utils.CloseStream.closeReadLocal() Job ERROR 关闭流异常!! \r\n " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + e.getMessage() + e.getStackTrace());

            e.printStackTrace();
        }

    }
    public static void closeReadHDFS(ReadHDFSModel readHDFSModel) {
        if (readHDFSModel == null) {
            LOG.warn("readHDFSModel == null");
            SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.utils.CloseStream.closeReadHDFS" ,  "cn.qtech.bigdata.utils.CloseStream.closeReadHDFS() Job warn \r\n readHDFSModel=null"+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) );

            return;
        }
        try {
            readHDFSModel.getBufferedReader().close();
            readHDFSModel.getInputStreamReader().close();
            readHDFSModel.getFSDataInputStream().close();
        } catch (IOException e) {
            LOG.error("closeReadHDFS文件异常!!");
            SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.utils.CloseStream.closeReadHDFS" ,  "cn.qtech.bigdata.utils.CloseStream.closeReadHDFS() Job ERROR 关闭流异常!! \r\n readHDFSModel:"+readHDFSModel + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + e.getMessage() + e.getStackTrace());

            e.printStackTrace();
        }

    }

    public static void  closeWriteHDFS(WriteHDFSModel writeHDFSModel) {
        if (writeHDFSModel == null) {
            LOG.warn("writeHDFSModel == null");
            SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.utils.CloseStream.closeWriteHDFS" ,  "cn.qtech.bigdata.utils.CloseStream.closeWriteHDFS() Job warn \r\n writeHDFSModel=null"+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) );

            return;
        }
        try {
            writeHDFSModel.getBufferedWriter().close();
            writeHDFSModel.getOutputStreamWriter().close();
            writeHDFSModel.getFsDataOutputStream().close();
        } catch (IOException e) {
           SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.utils.CloseStream.closeWriteHDFS" ,  "cn.qtech.bigdata.utils.CloseStream.closeWriteHDFS() Job ERROR 关闭流异常!! \r\n writeHDFSModel:"+writeHDFSModel + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + e.getMessage() + e.getStackTrace());

            e.printStackTrace();
        }

    }






    public static void closeBatchWriteLocal(List<WriteLocalModel> writeLocalList) {
        if (writeLocalList == null) {
            LOG.warn("writeLocalModel == null");
            SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.utils.CloseStream.closeBatchWriteLocal" ,  "cn.qtech.bigdata.utils.CloseStream.closeBatchWriteLocal() Job warn \r\n writeLocalModel=null"+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) );

            return;
        }
        try {
            for(WriteLocalModel writeLocalModel:writeLocalList){
                writeLocalModel.getBufferedWriter().close();
                writeLocalModel.getOutputStreamWriter().close();
                writeLocalModel.getFileOutputStream().close();
            }

        } catch (IOException e) {
            LOG.error("关闭写本地文件异常!!");
            SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.utils.CloseStream.closeBatchWriteLocal" ,  "cn.qtech.bigdata.utils.CloseStream.closeBatchWriteLocal() Job ERROR 关闭流异常!! \r\n " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + e.getMessage() + e.getStackTrace());

            e.printStackTrace();
        }

    }






}
