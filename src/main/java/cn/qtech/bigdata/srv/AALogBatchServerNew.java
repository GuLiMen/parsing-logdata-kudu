package cn.qtech.bigdata.srv;

import cn.qtech.bigdata.core.batch.BatchAALogEngineNew;
import cn.qtech.bigdata.model.RecordResponse;
import cn.qtech.bigdata.model.bufferStream.ReadLocalModel;
import cn.qtech.bigdata.utils.CloseStream;
import cn.qtech.bigdata.utils.FileSystemManager;
import cn.qtech.bigdata.utils.OperateFie;

import javax.management.monitor.Monitor;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static cn.qtech.bigdata.comm.AppConstants.RESULT_OUTPUT_LOCAL_DIR;

public class AALogBatchServerNew {


    public static void main(String[] args) throws IOException, InterruptedException {

/*        String extractLogFile = System.getProperty("extractLogFile");
        int count = Integer.parseInt(System.getProperty("count"));
        int totalThreadNum = Integer.parseInt(System.getProperty("totalThreadNum"));*/
        String extractLogFile = "D:\\aaupsert\\AANotReadLog";
        int count = 1000;
        int totalThreadNum = 1;
        OperateFie.createBatchFile(RESULT_OUTPUT_LOCAL_DIR, totalThreadNum);
        FileSystemManager.createFileSystemPool(totalThreadNum);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                totalThreadNum,
                totalThreadNum,
                100L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(count),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
        String HDFSpath;
        ReadLocalModel readPathLocalModel = new OperateFie().readLocalFile(extractLogFile);
        final CountDownLatch latch = new CountDownLatch(count);

        while ((HDFSpath = readPathLocalModel.getBufferedReader().readLine()) != null) {
            String finalHDFSpath = HDFSpath;

            executor.execute(() -> {
                BatchAALogEngineNew batchAALogEngineNew = new BatchAALogEngineNew();

                RecordResponse response = batchAALogEngineNew.getLogFile(finalHDFSpath,totalThreadNum);
                batchAALogEngineNew.parseLog(response,totalThreadNum);
                latch.countDown();//当前线程调用此方法，则计数减一
            });

        }

        try {
            latch.await();//阻塞当前线程，直到计数器的值为0
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
         /*   while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
            }*/
            executor.shutdown();
            CloseStream.closeReadLocal(readPathLocalModel);
            FileSystemManager.closeFileSystemPool(totalThreadNum);
        }


    }

}


