import cn.qtech.bigdata.core.parser.ParseLog;
import cn.qtech.bigdata.model.AtypiaProcess;
import cn.qtech.bigdata.model.RecordResponse;
import cn.qtech.bigdata.model.bufferStream.ReadHDFSModel;
import cn.qtech.bigdata.model.bufferStream.WriteHDFSModel;
import cn.qtech.bigdata.utils.CloseStream;
import cn.qtech.bigdata.utils.FileSystemManager;
import cn.qtech.bigdata.utils.OperateFie;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Pattern;

import static cn.qtech.bigdata.comm.AppConstants.BACKFILE_ROOTPATH;
import static cn.qtech.bigdata.comm.AppConstants.RESULT_OUTPUT_LOCAL_DIR;

public class BatchAALogEngineTest {


    // SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    private final Logger LOG = LoggerFactory.getLogger(BatchAALogEngineTest.class);
    final Pattern pattern = Pattern.compile(".*-[0-9]+-[0-9]+-[dDnN]-.*");

    // @Override
    public RecordResponse getLogFile(String numLogDir) {
        RecordResponse recordResponse = new RecordResponse();
        OperateFie operateFie = new OperateFie();
        recordResponse.setOperability(false);
        String threadName = Thread.currentThread().getName();
        String threadID = threadName.substring(threadName.indexOf("ad-") + 3);

        FileSystem fileSystem = new FileSystemManager().getFileSystem(Integer.parseInt(threadID));
        if (fileSystem == null || numLogDir == null || StringUtils.isBlank(numLogDir)) {
            LOG.error("fileSystem == null");
            return recordResponse;
        }
        String bakpath = BACKFILE_ROOTPATH + numLogDir;
        recordResponse.setNumLogDir("D:\\AA");
        recordResponse.setBakpath(bakpath);
        File numLogBakDir = new File(bakpath);

        //删除存在的备份文件
        if (numLogBakDir.isDirectory() && numLogBakDir.exists()) {
            for (File subTotal : numLogBakDir.listFiles()) {
                File subTotalPath = new File(subTotal.getAbsolutePath());
                if (subTotalPath.isDirectory()) {
                    for (File subfile : subTotalPath.listFiles()) {
                        subfile.delete();
                    }
                }
                subTotalPath.delete();
            }
            numLogBakDir.delete();
        }

        //获取文件名称
        ArrayList<LocatedFileStatus> fileStatusList = new ArrayList<>();
        try {
            RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(new Path(numLogDir), false);

            while (remoteIterator.hasNext()) {
                LocatedFileStatus fileStatus = remoteIterator.next();
                if (fileStatus.isFile()) {
                    fileStatusList.add(fileStatus);
                } else if (fileStatus.isDirectory()) {
                    LOG.warn("文件夹:" + numLogDir + "有子文件夹");
                }
            }
            if (fileStatusList.size() == 0) {
                fileSystem.delete(new Path(numLogDir), true);
                LOG.warn("fileStatusList.size() == 0" + numLogDir);
                return recordResponse;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        long beforeNow10min = new Date().getTime() - 600000;
        long beforeNow1h = new Date().getTime() - 3600000;

        if (fileStatusList.size() == 1) {
            long modificationTime = fileStatusList.get(0).getModificationTime();
            if (modificationTime > beforeNow10min || (fileStatusList.get(0).getPath().toString().endsWith(".tmp") && modificationTime > beforeNow1h)) {
                // LOG.info(numLogDir + "文件10min内修改过...不解析");
                return recordResponse;
            }
            String resultReadFile = fileStatusList.get(0).getPath().toString();
            recordResponse.setOperability(true);
            recordResponse.setResultReadFile(resultReadFile);
        } else if (fileStatusList.size() > 1) {
            /*
            多个文件,合并
            * */
            String resultReadFile = numLogDir + "/FlumeDataTotal";
            try {
                if (fileStatusList.size() > 8) {
                    fileSystem.moveToLocalFile(new Path(numLogDir), new Path(bakpath));

                    return recordResponse;
                }
                if (fileSystem.exists(new Path(resultReadFile))) {
                    fileSystem.delete(new Path(resultReadFile), false);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            String shardLine;

            ArrayList<Long> modificationList = new ArrayList<>();
            ArrayList<Long> blockSizeList = new ArrayList<>();
            HashMap<Long, Path> longPathHashMap = new HashMap<>();

            for (LocatedFileStatus fileStatus : fileStatusList) {
                long modificationTime = fileStatus.getModificationTime();
                blockSizeList.add(fileStatus.getBlockSize());
                longPathHashMap.put(modificationTime, fileStatus.getPath());
                modificationList.add(modificationTime);

            }
            if (blockSizeList.get(0) == blockSizeList.get(1)) {
                String readFile = fileStatusList.get(0).getPath().toString();
                recordResponse.setOperability(true);
                recordResponse.setResultReadFile(readFile);
                return recordResponse;
            }

            Collections.sort(modificationList);
            Long lastModificationTime = modificationList.get(modificationList.size() - 1);

            if (lastModificationTime > beforeNow10min || (longPathHashMap.get(lastModificationTime).toString().endsWith(".tmp") && lastModificationTime > beforeNow1h)) {
                // LOG.info(numLogDir + "文件10min内修改过...不解析");
                return recordResponse;
            }

            //合并碎文件   创建hdfs文件:  numLogDir + "/FlumeDataTotal"

            try {

                WriteHDFSModel mergeWriteHDFS = operateFie.writeHDFSFile(fileSystem, resultReadFile, true);

                for (int i = 0; i < modificationList.size(); i++) {
                    String tmpPath = longPathHashMap.get(modificationList.get(i)).toString();
                    //读取hdfs碎文件
                    ReadHDFSModel shardReadHDFSModel = operateFie.readHDFSFile(fileSystem, tmpPath);
                    if (shardReadHDFSModel == null) {
                        LOG.error("shardReadHDFSModel == null" + numLogDir);
                        return recordResponse;
                    }
                    //按行读取
                    while ((shardLine = shardReadHDFSModel.getBufferedReader().readLine()) != null) {
                        mergeWriteHDFS.getBufferedWriter().append(shardLine);
                        mergeWriteHDFS.getBufferedWriter().newLine();
                    }

                    mergeWriteHDFS.getBufferedWriter().flush();
                    CloseStream.closeReadHDFS(shardReadHDFSModel);
                }
                CloseStream.closeWriteHDFS(mergeWriteHDFS);
                recordResponse.setOperability(true);
                recordResponse.setResultReadFile(resultReadFile);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return recordResponse;

    }

    // @Override
    public void parseLog(RecordResponse recordResponse) {
        if (!recordResponse.isOperability()) {
            return;
        }
        OperateFie operateFie = new OperateFie();

        String numLogDir = recordResponse.getNumLogDir();
        String bakpath = recordResponse.getBakpath();
        String resultReadFile = recordResponse.getResultReadFile();
        String line;
        JSONObject resultMap = new JSONObject(100);
        String threadName = Thread.currentThread().getName();
        String threadID = threadName.substring(threadName.indexOf("ad-") + 3);

        FileSystem fileSystem = new FileSystemManager().getFileSystem(Integer.parseInt(threadID));
        //不符合规则log写入文件
        ReadHDFSModel parsereadHDFSModel = operateFie.readHDFSFile(fileSystem, resultReadFile);

        if (parsereadHDFSModel == null) {
            LOG.error("parsereadHDFSModel == null" + resultReadFile);
            return;
        }


        /*
         * 解析log
         */
        try {
            ParseLog parseLog = new ParseLog();
            AtypiaProcess atypiaProcess = new AtypiaProcess();
            while ((line = parsereadHDFSModel.getBufferedReader().readLine()) != null) {
                if (line.startsWith("STATION\tOnload\tLensTray")) {
                    fileSystem.delete(new Path(numLogDir), true);
                    //  // fileSystem.close();
                    return;
                }
                if (line.contains("00000000000000000")) {

                    continue;
                }
                String[] lineArr = line.split("\t");
                if (lineArr.length > 2 && lineArr[1] != null && lineArr[2] != null) {
                    JSONObject parseResult = parseLog.parseData(lineArr,atypiaProcess,resultReadFile);
                    resultMap.putAll(parseResult);
                }
            }
            String sid = resultMap.getString("SID");
            String MAIN_AA_START = resultMap.getString("MAIN_AA_START");


            String rowValue = sid;
            if (MAIN_AA_START != null && sid != null && !StringUtils.isBlank(sid)) {
                rowValue = new StringBuilder(MAIN_AA_START.replaceAll("\\s+|-|:", "")).reverse().toString().substring(0, 8).concat("_").concat(sid);
            } else if (sid == null || StringUtils.isBlank(sid)) {


                if (resultMap.getString("PRE_TEST_INIT") == null || "fail".equals(resultMap.getString("PRE_TEST_INIT").toLowerCase())) {
                    //INIT 失败或者没有,点亮失败

                    fileSystem.moveToLocalFile(new Path(numLogDir), new Path(bakpath));

                    //   // fileSystem.close();
                    return;

                }
                sid = (int) (Math.random() * 10) + "" + (int) (Math.random() * 10) + "" + (int) (Math.random() * 10) + "_rd" + System.currentTimeMillis() + "";
                rowValue = sid;
            }

            if (sid.contains("000000000001000000") || sid.contains("00000000000000000") || sid.contains("0000000001FF01") || sid.contains("FFE8FFE800000")
                    || sid.contains("FF01FF01FF01") || sid.contains("FF77FF77FF77") || sid.contains("FFE8FFE8FFE8") || sid.contains("007700770077")
                    || sid.contains("750075007500") || sid.contains("740074007400") || sid.contains("100010001000") || sid.contains("0505050505")) {
                LOG.error("sid 不规范 !!" + resultReadFile);

            }

            resultMap.put("ROWKEY", rowValue);


            /*
             *通过path获取厂区 生产线 基本信息
             * */
            String[] pathStrArr = resultReadFile.replaceAll("hdfs://nameservice", "").split("/");
            if (pathStrArr.length < 9) {

                LOG.error("pathStrArr.length < 9 文件路径错误 !!" + resultReadFile);
                return;
            }

            String EID = pathStrArr[4];
            String area = pathStrArr[2];
            String COB = pathStrArr[3];
            String compositeParam = pathStrArr[6].replaceAll("\\.|-+", "-");
            if ("GuCheng".equals(area)) {
                //古城机台编号是1-1（1线第一台） 5-4(5线第四台)
                compositeParam = compositeParam.replaceFirst("-", "_");
            }
            String formatCompositeParam = compositeParam;
            String specialProcess = "";
            if (pattern.matcher(compositeParam).matches()) {
                //路徑中有备注情况
                int idx = compositeParam.toUpperCase().indexOf("-N-");
                if (idx == -1) {
                    idx = compositeParam.toUpperCase().indexOf("-D-");
                }
                formatCompositeParam = compositeParam.substring(0, idx + 2);
                specialProcess = compositeParam.substring(idx + 3);
            }

            String[] vars = formatCompositeParam.split("-");
            String DEVICE_NUM = vars[0];
            String process_type = vars[1];
            String date_dir = vars[2].concat("-").concat(vars[3]);
            String working_shift = "";

            if (vars.length > 4) {
                working_shift = vars[4];
            }
            //机种带"-"的情况
            if ((vars.length == 6 && (formatCompositeParam.endsWith("D") || formatCompositeParam.endsWith("N") || formatCompositeParam.endsWith("d") || formatCompositeParam.endsWith("n")))
                    || (vars.length == 5 && !formatCompositeParam.endsWith("D") && !formatCompositeParam.endsWith("N") && !formatCompositeParam.endsWith("d") && !formatCompositeParam.endsWith("n"))) {
                process_type = vars[1].concat("-").concat(vars[2]);
                date_dir = vars[3].concat("-").concat(vars[4]);
                if (vars.length > 5) {
                    working_shift = vars[5];
                }
            }

            resultMap.put("area", area);
            resultMap.put("COB", COB);
            resultMap.put("EID", EID);
            resultMap.put("DEVICE_NUM", DEVICE_NUM);
            resultMap.put("process_type", process_type.replaceAll("o|O", "0"));
            resultMap.put("date_dir", date_dir);
            resultMap.put("pathdate", date_dir + "-" + working_shift);
            resultMap.put("specialProcess", specialProcess);
            resultMap.put("working_shift", working_shift);
            resultMap.put("file", pathStrArr[8]);

            /*
             * 路径值效验  长度效验 日期机种值效验
             * */
            if (area == null || StringUtils.isBlank(area) || EID == null || StringUtils.isBlank(EID) || COB == null || StringUtils.isBlank(COB) || DEVICE_NUM == null || StringUtils.isBlank(DEVICE_NUM)) {
                LOG.warn("parseLog() Job WAIN 异常  警告原因:area == null || StringUtils.isBlank(area) ||EID == null || StringUtils.isBlank(EID)  ||COB == null || StringUtils.isBlank(COB)||DEVICE_NUM == null || StringUtils.isBlank(DEVICE_NUM) \r\n area= " + area + "\r\n EID=" + EID + "\r\n COB=" + COB + "\r\n DEVICE_NUM=" + DEVICE_NUM + "\r\n" + resultReadFile);

            }
            if (vars.length < 3 || StringUtils.isBlank(process_type) || StringUtils.isBlank(date_dir) || !process_type.matches("^[a-zA-Z].*$") || !date_dir.matches("^[0-9].*$")) {
                LOG.warn("parseLog() Job WAIN", this.getClass().getName() + ".parseLog() Job WAIN 异常  警告原因:vars.length < 4 || StringUtils.isBlank(process_type) || StringUtils.isBlank(date_dir)|| !process_type.matches(\"^[a-zA-Z].*$\") || !date_dir.matches(\"^[0-9].*$\") \") \r\n vars.length= " + vars.length + "\r\n process_type=" + process_type + "\r\n date_dir=" + date_dir + "\r\n" + resultReadFile + "\r\n");


                //  SendEMailWarning.sendMail(RECEIVE_EMAIL, this.getClass().getName() + ".parseLog() Job WAIN", this.getClass().getName() + ".parseLog() Job WAIN 异常  警告原因:vars.length < 4 || StringUtils.isBlank(process_type) || StringUtils.isBlank(date_dir)|| !process_type.matches(\"^[a-zA-Z].*$\") || !date_dir.matches(\"^[0-9].*$\") \") \r\n vars.length= " + vars.length + "\r\n process_type=" + process_type + "\r\n date_dir=" + date_dir + "\r\n" + resultReadFile + "\r\n" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
            }

            /*
             * upsertSQL及column写出到文件
             * */



            //创建写入流


            //log移入备份盘
            fileSystem.moveToLocalFile(new Path(numLogDir), new Path(bakpath));
            System.out.println("---");
            String writeSql = RESULT_OUTPUT_LOCAL_DIR + "/upsertSQL_" + threadID + ".sql";
            String writeCol = RESULT_OUTPUT_LOCAL_DIR + "/column_" + threadID;
        } catch (IOException e) {
          // LOG.error("程序异常!! ");
            System.out.println(e.getMessage()+"=============");

            //   SendEMailWarning.sendMail(RECEIVE_EMAIL, this.getClass().getName() + ".parseLog() Job ERROR", this.getClass().getName() + ".parseLog() Job ERROR 异常 \r\n " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + e.getMessage() + e.getStackTrace() + "\r\n" + e.getCause());
            e.printStackTrace();
        }

    }
}

