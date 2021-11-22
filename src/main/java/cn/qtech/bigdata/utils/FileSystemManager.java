package cn.qtech.bigdata.utils;

import cn.qtech.bigdata.comm.SendEMailWarning;
import cn.qtech.bigdata.core.batch.BatchAALogEngineNew;
import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;


@Getter
public class FileSystemManager {
    private final static List<FileSystem> fileSystemPool = new ArrayList<>();

    private final static Logger LOG = LoggerFactory.getLogger(FileSystemManager.class);


    public static void createFileSystemPool(int size) {
        for (int i = 0; i <= size; i++) {
            try {
                FileSystem fileSystem = FileSystem.newInstance(getConf());
                fileSystemPool.add(fileSystem);
            } catch (IOException e) {
                LOG.error("createFileSystemPool 失败 !!");
                e.printStackTrace();
            }
        }
    }


    private static Configuration getConf() {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.defaultFS", "hdfs://nameservice");
        conf.set("dfs.nameservices", "nameservice");
        conf.set("dfs.ha.namenodes.nameservice", "bigdata01,bigdata02");
        conf.set("dfs.namenode.rpc-address.nameservice.bigdata01", "10.170.3.11:8020");
        conf.set("dfs.namenode.rpc-address.nameservice.bigdata02", "10.170.3.12:8020");
        conf.set("dfs.client.failover.proxy.provider.nameservice",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("fs.hdfs.impl.disable.cache", "true");
        return conf;
    }


    public FileSystem getFileSystem() {
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(getConf());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return fileSystem;
    }

    public FileSystem getFileSystem(int i) {

        return fileSystemPool.get(i);
    }


    public static void closeFileSystemPool(int size) {

        for (int i = 0; i <= size; i++) {
            try {
                fileSystemPool.get(i).close();
            } catch (IOException e) {
                LOG.error("closeFileSystemPool 失败 !!");
            }
        }

    }


}
