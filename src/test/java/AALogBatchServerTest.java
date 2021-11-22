import cn.qtech.bigdata.core.batch.BatchAALogEngineNew;
import cn.qtech.bigdata.model.RecordResponse;
import cn.qtech.bigdata.model.bufferStream.ReadLocalModel;
import cn.qtech.bigdata.utils.CloseStream;
import cn.qtech.bigdata.utils.FileSystemManager;
import cn.qtech.bigdata.utils.OperateFie;

import java.util.concurrent.*;

import static cn.qtech.bigdata.comm.AppConstants.RESULT_OUTPUT_LOCAL_DIR;

public class AALogBatchServerTest {


    public static void main(String[] args) throws Exception {


        String extractLogFile = "D:\\AANotReadLog";
        int count = 1000;
        int totalThreadNum = 16;
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
                BatchAALogEngineTest test = new BatchAALogEngineTest();

                RecordResponse response = test.getLogFile(finalHDFSpath);
                test.parseLog(response);
                latch.countDown();//当前线程调用此方法，则计数减一
            });

        }

        latch.await();//阻塞当前线程，直到计数器的值为0
        Thread.sleep(8000);
        CloseStream.closeReadLocal(readPathLocalModel);
        executor.shutdown();
        FileSystemManager.closeFileSystemPool(totalThreadNum);
        // fileSystem.close();
        //    System.out.println(df.format(new Date()) + "-------");


    }

}


