package cn.qtech.bigdata.srv;

import cn.qtech.bigdata.comm.SendEMailWarning;
import cn.qtech.bigdata.utils.FileSystemManager;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashSet;

import static cn.qtech.bigdata.comm.AppConstants.RECEIVE_EMAIL;

/**
 * 删除HDFS /flume 空文件夹
 */
public class RemoveHDFSEmptyDir extends FileSystemManager {
    private static final String traversalDir = "/flume";

    public static void main(String[] args) throws IOException {
        FileSystem fileSystem = new FileSystemManager().getFileSystem();

        if (fileSystem == null) {
            SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.srv.TraversalHDFSFiles.main() Job ERROR", "cn.qtech.bigdata.srv.TraversalHDFSFiles.main()  \n fileSystem ==null  \r\n " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
            return;
        }

        LocalDateTime dateTime = LocalDateTime.now();
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("MMdd");
        String format = dateTime.format(dateTimeFormatter);


        HashSet<Path> allPathSet = new HashSet<>();
        HashSet<Path> notEmptyPathSet = new HashSet<>();
        Long threshold = 24L * 60 * 60 * 1000 * 3; //3d

        for (FileStatus area : fileSystem.listStatus(new Path(traversalDir))) {
            for (FileStatus cob : fileSystem.listStatus(new Path(area.getPath().toString()))) {

                for (FileStatus EQcode : fileSystem.listStatus(new Path(cob.getPath().toString()))) {
                    for (FileStatus lot : fileSystem.listStatus(new Path(EQcode.getPath().toString()))) {
                        for (FileStatus composite : fileSystem.listStatus(new Path(lot.getPath().toString()))) {

                            //  System.out.println(composite.getPath() + "==" + LocalDateTime.ofEpochSecond(composite.getModificationTime() / 1000, 0, ZoneOffset.ofHours(8)) + "--" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(composite.getModificationTime() - threshold)+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis() - threshold));
                            if (new Date().getTime() - threshold > composite.getModificationTime()) {
                                //所有3天前的文件夹
                                allPathSet.add(composite.getPath());
                                boolean flag = true;
                                for (FileStatus unit : fileSystem.listStatus(new Path(composite.getPath().toString()))) {
                                    if (flag) {
                                        for (FileStatus numLog : fileSystem.listStatus(new Path(unit.getPath().toString()))) {
                                            notEmptyPathSet.add(composite.getPath());
                                            flag = false;
                                        }
                                    }

                                }
                            }
                        }
                    }
                }

            }
        }


      //  System.out.println("total:" + allPathSet.size());
        allPathSet.removeAll(notEmptyPathSet);
        //System.out.println(" total remove after" + allPathSet.size());

       // System.out.println("notEmptyPathSet" + notEmptyPathSet.size());


        allPathSet.stream().forEach(e->{
            try {
                System.out.println("remove dir:"+e);
//                fileSystem.delete(e,true);
                fileSystem.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        });



    }


}
