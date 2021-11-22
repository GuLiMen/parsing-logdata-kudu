package cn.qtech.bigdata.utils;

import cn.qtech.bigdata.comm.SendEMailWarning;
import cn.qtech.bigdata.model.bufferStream.WriteLocalModel;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static cn.qtech.bigdata.comm.AppConstants.RECEIVE_EMAIL;
import static org.apache.hadoop.hdfs.server.namenode.MetaRecoveryContext.LOG;

public class OperateStream {
    public static void flush(List<WriteLocalModel> writeLocalList) {
        if (writeLocalList == null) {
            LOG.warn("writeLocalList == null");
            SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.utils.OperateStream" ,  "cn.qtech.bigdata.utils.OperateStream.flush() Job warn \r\n writeLocalList=null"+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) );

            return;
        }
        try {
            for(WriteLocalModel writeLocalModel:writeLocalList){
                writeLocalModel.getBufferedWriter().flush();

            }

        } catch (IOException e) {
            LOG.error("flush文件异常!!");
            SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.utils.OperateStream" ,  "cn.qtech.bigdata.utils.OperateStream.flush() Job ERROR flush文件异常异常!! \r\n " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + e.getMessage() + e.getStackTrace());

            e.printStackTrace();
        }

    }
}
