package cn.qtech.bigdata.utils;

import cn.qtech.bigdata.comm.SendEMailWarning;
import cn.qtech.bigdata.model.bufferStream.ReadHDFSModel;
import cn.qtech.bigdata.model.bufferStream.ReadLocalModel;
import cn.qtech.bigdata.model.bufferStream.WriteHDFSModel;
import cn.qtech.bigdata.model.bufferStream.WriteLocalModel;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static cn.qtech.bigdata.comm.AppConstants.*;
import static org.apache.hadoop.hdfs.server.namenode.MetaRecoveryContext.LOG;

public class OperateFie {
    private static final Logger LOG = LoggerFactory.getLogger(OperateFie.class);
    static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

    public ReadLocalModel readLocalFile(String readFile) {
        ReadLocalModel readLocalModel = new ReadLocalModel();
        try {
            FileInputStream fileInputStream = new FileInputStream(readFile);
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            readLocalModel.setBufferedReader(bufferedReader);
            readLocalModel.setInputStreamReader(inputStreamReader);
            readLocalModel.setFileInputStream(fileInputStream);

        } catch (FileNotFoundException e) {
            LOG.error("读本地文件异常!!文件名为:" + readFile);
            SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.utils.OperatorFile.readLocalFile", "cn.qtech.bigdata.utils.OperatorFile.readLocalFile() Job ERROR 读本地文件异常!! \r\n 文件:" + readFile + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + e.getMessage() + e.getStackTrace());

            e.printStackTrace();
        }
        return readLocalModel;
    }

    public WriteLocalModel writeLocalFile(String writeFile, boolean isAppend) {
        WriteLocalModel writeLocalModel = new WriteLocalModel();
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(writeFile, isAppend);
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream);
            BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter);
            writeLocalModel.setFileOutputStream(fileOutputStream);
            writeLocalModel.setOutputStreamWriter(outputStreamWriter);
            writeLocalModel.setBufferedWriter(bufferedWriter);
        } catch (FileNotFoundException e) {
            LOG.error("写本地文件异常!!文件名为:" + writeFile);
            SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.utils.OperatorFile.writeLocalFile", "cn.qtech.bigdata.utils.OperatorFile.writeLocalFile() Job ERROR 写本地文件异常!! \r\n 文件:" + writeFile + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + e.getMessage() + e.getStackTrace());

            e.printStackTrace();
        }
        return writeLocalModel;

    }

    public ReadHDFSModel readHDFSFile(FileSystem fileSystem, String readFile) {
        ReadHDFSModel readHDFSModel = new ReadHDFSModel();
        try {
            FSDataInputStream fSDataInputStream = fileSystem.open(new Path(readFile));
            InputStreamReader inputStreamReader = new InputStreamReader(fSDataInputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            readHDFSModel.setFSDataInputStream(fSDataInputStream);
            readHDFSModel.setInputStreamReader(inputStreamReader);
            readHDFSModel.setBufferedReader(bufferedReader);
        } catch (IOException e) {

            new OperateFie().recoverLease(fileSystem, readFile, false);
            return null;

        }
        return readHDFSModel;
    }

    public void recoverLease(FileSystem fileSystem, String readFile, boolean isMove) {
        try {
            InetSocketAddress addressOfActive = HAUtil.getAddressOfActive(fileSystem);
            DistributedFileSystem dfs = new DistributedFileSystem();
            dfs.initialize(URI.create("hdfs://" + addressOfActive.getHostName() + ":" + addressOfActive.getPort()), fileSystem.getConf());
            dfs.setConf(fileSystem.getConf());

            if (isMove) {
                RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(new Path(readFile), false);
                while (remoteIterator.hasNext()) {
                    LocatedFileStatus fileStatus = remoteIterator.next();
                    if (fileStatus.isFile()) {
                        String moveFile = fileStatus.getPath().toString().replaceFirst("hdfs://nameservice", "");
                        fileSystem.setPermission(new Path(moveFile), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
                        dfs.recoverLease(new Path(moveFile));
                    }
                }
                fileSystem.moveToLocalFile(new Path(readFile), new Path(BACKFILE_ROOTPATH + readFile));
                dfs.close();
                return;
            }
            //更改权限
            fileSystem.setPermission(new Path(readFile), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(readFile).getParent());
            long modificationTime = new Date().getTime();
            for (FileStatus s : fileStatuses) {
                modificationTime = s.getModificationTime();
            }
            long beforeNow1h = new Date().getTime() - 3600000;
            if (modificationTime < beforeNow1h) {
                //释放租约
                dfs.recoverLease(new Path(readFile.replaceFirst("hdfs://nameservice", "")));
                LOG.info("释放租约" + readFile);
                dfs.close();
            }

        } catch (IOException ex) {
            LOG.error("释放租约异常 !!" + ex.getStackTrace());
            ex.printStackTrace();
        }
    }

    public WriteHDFSModel writeHDFSFile(FileSystem fileSystem, String writeFile, boolean isCreateFile) {
        WriteHDFSModel writeHDFSModel = new WriteHDFSModel();
        FSDataOutputStream fsDataOutputStream;
        try {
            if (isCreateFile == true) {
                fsDataOutputStream = fileSystem.create(new Path(writeFile), true);
            } else {
                fsDataOutputStream = fileSystem.append(new Path(writeFile));
            }
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fsDataOutputStream);
            BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter);
            writeHDFSModel.setFsDataOutputStream(fsDataOutputStream);
            writeHDFSModel.setOutputStreamWriter(outputStreamWriter);
            writeHDFSModel.setBufferedWriter(bufferedWriter);
        } catch (IOException e) {
            LOG.error("写HDFS文件异常!!文件名为:" + writeFile);
            SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.utils.OperatorFile.writeHDFSFile", "cn.qtech.bigdata.utils.OperatorFile.writeHDFSFile() Job ERROR 写HDFS文件异常!! \r\n 文件:" + writeFile + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + e.getMessage() + e.getStackTrace());
            e.printStackTrace();
        }
        return writeHDFSModel;
    }

    public static void createNewFile(List<String> fileList) {

        if (fileList == null) {
            LOG.warn("fileList == null");
            SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.utils.OperateFie", "cn.qtech.bigdata.utils.OperateFie.createNewFile() Job warn \r\n fileList=null" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));

            return;
        }
        try {
            for (String fileNmae : fileList) {
                new File(fileNmae).createNewFile();
            }
        } catch (IOException e) {
            LOG.warn("createNewFile!!");
            //   SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.utils.OperateFie", "cn.qtech.bigdata.utils.OperateFie.createNewFile() Job ERROR createNewFile!! \r\n " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + e.getMessage() + e.getStackTrace());

            // e.printStackTrace();
        }
    }

    public static void createBatchFile(String pathName, int total) throws IOException {
        for (int i = 0; i <= total; i++) {
           File SqlFile = new File(pathName + "/upsertSQL_" + i + ".sql");

            if (!SqlFile.exists()) {
                SqlFile.createNewFile();
            }
        }
    }

    public static void deleteFile(List<String> fileList) {

        if (fileList == null) {
            LOG.warn("fileList == null");
            // SendEMailWarning.sendMail(RECEIVE_EMAIL, "cn.qtech.bigdata.utils.OperateFie", "cn.qtech.bigdata.utils.OperateFie.createNewFile() Job warn \r\n fileList=null" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));

            return;
        }

        for (String fileNmae : fileList) {
            File file = new File(fileNmae);
            if (file.exists()) {
                file.delete();
            }
        }

    }


}
