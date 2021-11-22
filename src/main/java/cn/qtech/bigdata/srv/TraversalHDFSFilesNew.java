package cn.qtech.bigdata.srv;

import cn.qtech.bigdata.comm.AppConstants.*;
import cn.qtech.bigdata.comm.SendEMailWarning;
import cn.qtech.bigdata.model.bufferStream.WriteLocalModel;
import cn.qtech.bigdata.utils.CloseStream;
import cn.qtech.bigdata.utils.FileSystemManager;
import cn.qtech.bigdata.utils.OperateFie;
import cn.qtech.bigdata.utils.OperateStream;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static cn.qtech.bigdata.comm.AppConstants.*;

public class TraversalHDFSFilesNew extends FileSystemManager {
    private static AtomicInteger i = new AtomicInteger(0);

    public static void main(String[] args) throws IOException {
        //   SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        //   System.out.println(df.format(new Date()) + "start");
        FileSystem fileSystem = new FileSystemManager().getFileSystem();

        if (fileSystem == null) {
            SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.srv.TraversalHDFSFiles.main() Job ERROR", "cn.qtech.bigdata.srv.TraversalHDFSFiles.main()  \n fileSystem ==null  \r\n " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
            return;
        }

        String path01 = "D:\\aaupsert\\AANotReadLog";
//        String path01 = RESULT_OUTPUT_LOCAL_DIR + "/AANotReadLog_01";
//        String path02 = RESULT_OUTPUT_LOCAL_DIR + "/AANotReadLog_02";
//        String path03 = RESULT_OUTPUT_LOCAL_DIR + "/AANotReadLog_03";
//        String path04 = RESULT_OUTPUT_LOCAL_DIR + "/AANotReadLog_04";
//        String path05 = RESULT_OUTPUT_LOCAL_DIR + "/AANotReadLog_05";
//        String path06 = RESULT_OUTPUT_LOCAL_DIR + "/AANotReadLog_06";
//        String path07 = RESULT_OUTPUT_LOCAL_DIR + "/AANotReadLog_07";

        //创建文件
        List<String> fileList = Arrays.asList(path01);
//        OperateFie.deleteFile(fileList);
        OperateFie operateFie = new OperateFie();
        WriteLocalModel AA01Local = operateFie.writeLocalFile(path01, true);
//        WriteLocalModel AA02Local = operateFie.writeLocalFile(path02, true);
//        WriteLocalModel AA03Local = operateFie.writeLocalFile(path03, true);
//        WriteLocalModel AA04Local = operateFie.writeLocalFile(path04, true);
//        WriteLocalModel AA05Local = operateFie.writeLocalFile(path05, true);
//        WriteLocalModel AA06Local = operateFie.writeLocalFile(path06, true);
//        WriteLocalModel AA07Local = operateFie.writeLocalFile(path07, true);
        List<WriteLocalModel> writeLocalList = Arrays.asList(AA01Local);
//        List<WriteLocalModel> writeLocalList = Arrays.asList(AA01Local,AA02Local, AA03Local, AA04Local, AA05Local, AA06Local,AA07Local);


        int total = 7;
        //遍历HDFS文件分发
        for (FileStatus area : fileSystem.listStatus(new Path(HDFS_FLUME_DIR))) {
            if (area.getPath().getName().equals("GuCheng2")) {
                for (FileStatus cob : fileSystem.listStatus(new Path(area.getPath().toString()))) {
                    for (FileStatus EQcode : fileSystem.listStatus(new Path(cob.getPath().toString()))) {
                        for (FileStatus lot : fileSystem.listStatus(new Path(EQcode.getPath().toString()))) {
                            for (FileStatus composite : fileSystem.listStatus(new Path(lot.getPath().toString()))) {
                                for (FileStatus unit : fileSystem.listStatus(new Path(composite.getPath().toString()))) {
                                    for (FileStatus logdir : fileSystem.listStatus(new Path(unit.getPath().toString()))) {
                                        String numDir = logdir.getPath().toString();
                                        if (numDir.endsWith(".log")) {
                                            String logPath = numDir.replaceFirst("hdfs://nameservice", "");
                                            i.getAndIncrement();

//                                        if (i.get() % total == 0) {

                                            AA01Local.getBufferedWriter().append(logPath);
                                            AA01Local.getBufferedWriter().newLine();


//                                        } else if (i.get() % total == 1) {
//                                            AA02Local.getBufferedWriter().append(logPath);
//                                            AA02Local.getBufferedWriter().newLine();
//                                        } else if (i.get() % total == 2) {
//                                            AA03Local.getBufferedWriter().append(logPath);
//                                            AA03Local.getBufferedWriter().newLine();
//                                        } else if (i.get() % total == 3) {
//                                            AA04Local.getBufferedWriter().append(logPath);
//                                            AA04Local.getBufferedWriter().newLine();
//                                        } else if (i.get() % total == 4) {
//                                            AA05Local.getBufferedWriter().append(logPath);
//                                            AA05Local.getBufferedWriter().newLine();
//                                        } else if (i.get() % total == 5) {
//                                            AA06Local.getBufferedWriter().append(logPath);
//                                            AA06Local.getBufferedWriter().newLine();
//                                        } else if (i.get() % total == 6) {
//                                            AA07Local.getBufferedWriter().append(logPath);
//                                            AA07Local.getBufferedWriter().newLine();
//                                        }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        System.out.println("total=" + i.get());


        OperateStream.flush(writeLocalList);
        CloseStream.closeBatchWriteLocal(writeLocalList);

        fileSystem.close();
        //  System.out.println(df.format(new Date()) + "end");


    }


}
