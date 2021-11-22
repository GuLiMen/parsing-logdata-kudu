import cn.qtech.bigdata.comm.SendEMailWarning;
import cn.qtech.bigdata.model.bufferStream.WriteLocalModel;
import cn.qtech.bigdata.utils.CloseStream;
import cn.qtech.bigdata.utils.FileSystemManager;
import cn.qtech.bigdata.utils.OperateFie;
import cn.qtech.bigdata.utils.OperateStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static cn.qtech.bigdata.comm.AppConstants.*;

public class TraversalHDFSFilesTest extends FileSystemManager {
    private static AtomicInteger i = new AtomicInteger(0);

    public static void main(String[] args) throws IOException {
        //   SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        //   System.out.println(df.format(new Date()) + "start");
        FileSystem fileSystem = new FileSystemManager().getFileSystem();

        if (fileSystem == null) {
            SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.srv.TraversalHDFSFiles.main() Job ERROR", "cn.qtech.bigdata.srv.TraversalHDFSFiles.main()  \n fileSystem ==null  \r\n " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
            return;
        }


        //创建文件
        OperateFie operateFie = new OperateFie();
        String path01 = "D:\\AANotReadLog";
        WriteLocalModel AA01Local = operateFie.writeLocalFile(path01, true);


        int total = 8;
        //遍历HDFS文件分发
        for (FileStatus area : fileSystem.listStatus(new Path(HDFS_FLUME_DIR))) {
            for (FileStatus cob : fileSystem.listStatus(new Path(area.getPath().toString()))) {
                for (FileStatus EQcode : fileSystem.listStatus(new Path(cob.getPath().toString()))) {
                    for (FileStatus lot : fileSystem.listStatus(new Path(EQcode.getPath().toString()))) {
                        for (FileStatus composite : fileSystem.listStatus(new Path(lot.getPath().toString()))) {
                            for (FileStatus unit : fileSystem.listStatus(new Path(composite.getPath().toString()))) {
                                for (FileStatus logdir : fileSystem.listStatus(new Path(unit.getPath().toString()))) {
                                    String numDir = logdir.getPath().toString();
                                    if (numDir.endsWith(".log")) {
                                        String logPath = numDir.replaceFirst("hdfs://nameservice", "");
                                        if (i.get() == 5) {
                                            AA01Local.getBufferedWriter().flush();
                                            CloseStream.closeWriteLocal(AA01Local);
                                            return;
                                        }
                                        AA01Local.getBufferedWriter().append(logPath);
                                        AA01Local.getBufferedWriter().newLine();
                                        i.getAndIncrement();

                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        System.out.println("total=" + i.get());


        fileSystem.close();
        //  System.out.println(df.format(new Date()) + "end");


    }


}
