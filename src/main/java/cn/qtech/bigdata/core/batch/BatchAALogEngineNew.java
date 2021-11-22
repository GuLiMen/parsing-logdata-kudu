package cn.qtech.bigdata.core.batch;

import cn.qtech.bigdata.core.parser.ParseLog;
import cn.qtech.bigdata.model.AtypiaProcess;
import cn.qtech.bigdata.model.RecordResponse;
import cn.qtech.bigdata.model.RejectRateModel;
import cn.qtech.bigdata.model.bufferStream.ReadHDFSModel;
import cn.qtech.bigdata.model.bufferStream.WriteHDFSModel;
import cn.qtech.bigdata.model.bufferStream.WriteLocalModel;
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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cn.qtech.bigdata.comm.AppConstants.*;

public class BatchAALogEngineNew {


    // SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    private final Logger LOG = LoggerFactory.getLogger(BatchAALogEngineNew.class);
    final Pattern pattern = Pattern.compile(".*-[0-9]+-[0-9]+-[dDnN]-.*");

    public RecordResponse getLogFile(String numLogDir, int totalThreadNum) {
        RecordResponse recordResponse = new RecordResponse();
        OperateFie operateFie = new OperateFie();

        recordResponse.setOperability(false);
        String threadName = Thread.currentThread().getName();
        int threadID = Integer.parseInt(threadName.substring(threadName.indexOf("ad-") + 3)) % totalThreadNum;

        FileSystem fileSystem = new FileSystemManager().getFileSystem(threadID);
        if (fileSystem == null || numLogDir == null || StringUtils.isBlank(numLogDir)) {
            LOG.error("fileSystem == null");
            return recordResponse;
        }
        String bakpath = BACKFILE_ROOTPATH + numLogDir;
        recordResponse.setNumLogDir(numLogDir);
        recordResponse.setBakpath(bakpath);
        File numLogBakDir = new File(bakpath);

        //删除存在的备份文件
        if (numLogBakDir.isDirectory() && numLogBakDir.exists()) {
            for (File subTotal : numLogBakDir.listFiles()) {
                File subTotalPath = new File(subTotal.getAbsolutePath());
                if (subTotalPath.isDirectory()) {
                    for (File subfile : subTotalPath.listFiles()) {
//                        subfile.delete();
                    }
                }
//                subTotalPath.delete();
            }
//            numLogBakDir.delete();
        }

        //获取文件名称
        ArrayList<LocatedFileStatus> fileStatusList = new ArrayList<>();
        try {
            RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(new Path(numLogDir), false);

            while (remoteIterator.hasNext()) {
                LocatedFileStatus fileStatus = remoteIterator.next();
                if (fileStatus.isFile()) {
                    if (fileStatus.getPath().toString().endsWith("FlumeDataTotal")) {
//                        fileSystem.delete(new Path(fileStatus.getPath().toString()), false);
                        continue;
                    }
                    fileStatusList.add(fileStatus);
                } else if (fileStatus.isDirectory()) {
                    LOG.warn("文件夹:" + numLogDir + "有子文件夹");
                }
            }
            if (fileStatusList.size() == 0) {
//                fileSystem.delete(new Path(numLogDir), true);
                LOG.warn("fileStatusList.size() == 0" + numLogDir);
                return recordResponse;
            }
        } catch (IOException e) {
            LOG.error("遍历文件异常 !! 文件名:" + numLogDir + e.getMessage());
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
            WriteHDFSModel mergeWriteHDFS = operateFie.writeHDFSFile(fileSystem, resultReadFile, true);
            for (int i = 0; i < modificationList.size(); i++) {
                String tmpPath = longPathHashMap.get(modificationList.get(i)).toString();
                //读取hdfs碎文件
                ReadHDFSModel shardReadHDFSModel = operateFie.readHDFSFile(fileSystem, tmpPath);
                if (shardReadHDFSModel == null) {
                    //  LOG.warn("shardReadHDFSModel == null" + numLogDir);
                    return recordResponse;
                }
                try {
                    //按行读取
                    while ((shardLine = shardReadHDFSModel.getBufferedReader().readLine()) != null) {
                        mergeWriteHDFS.getBufferedWriter().append(shardLine);
                        mergeWriteHDFS.getBufferedWriter().newLine();
                    }
                    mergeWriteHDFS.getBufferedWriter().flush();
                    CloseStream.closeReadHDFS(shardReadHDFSModel);
                } catch (IOException ex) {
                    if (ex.getMessage().contains("Cannot obtain block")) {
                        operateFie.recoverLease(fileSystem, numLogDir, true);
                    }
                    LOG.error("合并 异常 !! 文件名:" + numLogDir + ex.getMessage());
                    ex.printStackTrace();
                    recordResponse.setOperability(false);
                    return recordResponse;

                }
            }
            CloseStream.closeWriteHDFS(mergeWriteHDFS);
            recordResponse.setOperability(true);
            recordResponse.setResultReadFile(resultReadFile);


        }

        return recordResponse;

    }

    // @Override
    public void parseLog(RecordResponse recordResponse, int totalThreadNum) {
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
        int threadID = Integer.parseInt(threadName.substring(threadName.indexOf("ad-") + 3)) % totalThreadNum;

        FileSystem fileSystem = new FileSystemManager().getFileSystem(threadID);
        //不符合规则log写入文件

        ReadHDFSModel parsereadHDFSModel = operateFie.readHDFSFile(fileSystem, resultReadFile);
        String[] prefix = {"ar_", "br_", "cr_", "dr_", "er_", "fr_", "gr_"};

        if (parsereadHDFSModel == null) {
            // LOG.warn("parsereadHDFSModel == null" + resultReadFile);
            return;
        }
//        WriteLocalModel contentErrorModel = operateFie.writeLocalFile("/data/workspace/alarm/AAlogContentError", true);
//        WriteLocalModel sidEmptyErrorModel = operateFie.writeLocalFile("/data/workspace/alarm/AAlogSidEmptyError", true);
//        WriteLocalModel sidAbnormalErrorModel = operateFie.writeLocalFile("/data/workspace/alarm/AAlogSidAbnormalError", true);
//        WriteLocalModel pathSizeLess9Model = operateFie.writeLocalFile("/data/workspace/alarm/pathSizeLess9Error", true);
//        WriteLocalModel pathValueEmptyModel = operateFie.writeLocalFile("/data/workspace/alarm/pathValueEmptyError", true);
//        WriteLocalModel pathValueNoMatchModel = operateFie.writeLocalFile("/data/workspace/alarm/pathValueNotMatchError", true);
//        WriteLocalModel sidEmptyAndInitFailModel = operateFie.writeLocalFile("/data/workspace/alarm/sidEmptyAndInitFail", true);
//        WriteLocalModel datepathSizeLess4Model = operateFie.writeLocalFile("/data/workspace/alarm/datepathSizeLess4Error", true);


        WriteLocalModel contentErrorModel = operateFie.writeLocalFile("D:/alarm/AAlogContentError", true);
        WriteLocalModel sidEmptyErrorModel = operateFie.writeLocalFile("D:/alarm/AAlogSidEmptyError", true);
        WriteLocalModel sidAbnormalErrorModel = operateFie.writeLocalFile("D:/alarm/AAlogSidAbnormalError", true);
        WriteLocalModel pathSizeLess9Model = operateFie.writeLocalFile("D:/alarm/pathSizeLess9Error", true);
        WriteLocalModel pathValueEmptyModel = operateFie.writeLocalFile("D:/alarm/pathValueEmptyError", true);
        WriteLocalModel pathValueNoMatchModel = operateFie.writeLocalFile("D:/alarm/pathValueNotMatchError", true);
        WriteLocalModel sidEmptyAndInitFailModel = operateFie.writeLocalFile("D:/alarm/sidEmptyAndInitFail", true);
        WriteLocalModel datepathSizeLess4Model = operateFie.writeLocalFile("D:/alarm/datepathSizeLess4Error", true);


        /*
         * 解析log
         */
        resultMap.put(SENSOR_THROW_TIGGER, "");
        resultMap.put(GRIPPEROPEN_CNT, 0);
        resultMap.put(SID_CNT, 1);
        String parseTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        resultMap.put("parse_time", parseTime);
        ParseLog parseLog = new ParseLog();
        AtypiaProcess atypiaProcess = new AtypiaProcess();
        try {
            while ((line = parsereadHDFSModel.getBufferedReader().readLine()) != null) {
                if (line.startsWith("STATION\tOnload\tLensTray")) {
//                    fileSystem.delete(new Path(numLogDir), true);
                    return;
                }
                if (line.contains("00000000000000000")) {
                    contentErrorModel.getBufferedWriter().write(resultReadFile);
                    contentErrorModel.getBufferedWriter().newLine();
                    contentErrorModel.getBufferedWriter().flush();
                    continue;
                }
                String[] lineArr = line.split("\t");
                if (lineArr.length > 2 && lineArr[1] != null && lineArr[2] != null) {
                    JSONObject parseResult = parseLog.parseData(lineArr, atypiaProcess, resultReadFile);
                    resultMap.putAll(parseResult);
                }
            }
        } catch (IOException e) {
            if (e.getMessage().contains("Cannot obtain block")) {
                operateFie.recoverLease(fileSystem, numLogDir, false);
            }
            LOG.error(".parseLog()解析异常 ! " + resultReadFile + e.getMessage());
            e.printStackTrace();
        }
        CloseStream.closeReadHDFS(parsereadHDFSModel);

        Integer gripperOpen_cnt = resultMap.getInteger(GRIPPEROPEN_CNT);
        if (gripperOpen_cnt == 1) {
            resultMap.put(SENSOR_THROW_TIGGER, "");
        }
        // 没有VCM_MOVE2，VCM_MOVE认为是VCM_MOVE2
        if ((!atypiaProcess.getVCM_Move2() && atypiaProcess.getVCM_Move() && !atypiaProcess.getVCM_Move1())
                || (!atypiaProcess.getVCM_Move2() && !atypiaProcess.getVCM_Move() && atypiaProcess.getVCM_Move1())) {
            resultMap.put(VCM_MOVE2_FAIL_CNT, resultMap.get(VCM_MOVE_FAIL_CNT));
            resultMap.put(VCM_MOVE_FAIL_CNT, 0);
            if (resultMap.get(SENSOR_THROW_TIGGER).equals("VCM_Move")) {
                resultMap.put(SENSOR_THROW_TIGGER, "VCM_Move2");
            }
        }
        // 如果MAIN_AA_START为空 重新赋值 优先级: MAIN_AA_START > Pre_Test_Start > Onload_TIME > current time
        if (resultMap.getString("MAIN_AA_START") == null || StringUtils.isBlank(resultMap.getString("MAIN_AA_START"))) {
            String PreTestStart = resultMap.getString("Pre_Test_Start");
            String tmpTime = PreTestStart == null || StringUtils.isBlank(PreTestStart) ? resultMap.getString("Onload_TIME") : PreTestStart;
            String mainStart = tmpTime == null || StringUtils.isBlank(tmpTime) ? parseTime : tmpTime;
            resultMap.put("MAIN_AA_START", mainStart);
        }
        resultMap.remove("Pre_Test_Start");
        resultMap.remove("Onload_TIME");
        /*
         *   检查sid
         **/
        String sid = resultMap.getString("SID");
        try {
            if (sid == null || StringUtils.isBlank(sid)) {
/*                if ("init".equals(resultMap.getString(SENSOR_THROW_TIGGER).toLowerCase())) {
                    //INIT 失败或者没有,点亮失败
                    fileSystem.moveToLocalFile(new Path(numLogDir), new Path(bakpath));
                    sidEmptyAndInitFailModel.getBufferedWriter().write(resultReadFile);
                    sidEmptyAndInitFailModel.getBufferedWriter().newLine();
                    sidEmptyAndInitFailModel.getBufferedWriter().flush();
                    CloseStream.closeWriteLocal(sidEmptyAndInitFailModel);
                    return;
                }*/
                resultMap.put("SID", prefix[new Random().nextInt(7)] + UUID.randomUUID().toString().replaceAll("-", ""));
                sidEmptyErrorModel.getBufferedWriter().write(resultReadFile);
                sidEmptyErrorModel.getBufferedWriter().newLine();
                sidEmptyErrorModel.getBufferedWriter().flush();
                CloseStream.closeWriteLocal(sidEmptyErrorModel);
            } else if (sid.contains("000000000001000000") || sid.contains("00000000000000000") || sid.contains("0000000001FF01") || sid.contains("FFE8FFE800000")
                    || sid.contains("FF01FF01FF01") || sid.contains("FF77FF77FF77") || sid.contains("FFE8FFE8FFE8") || sid.contains("007700770077")
                    || sid.contains("750075007500") || sid.contains("740074007400") || sid.contains("100010001000") || sid.contains("0505050505")) {
                LOG.error("sid 不规范 !!" + resultReadFile);
                sidAbnormalErrorModel.getBufferedWriter().write(resultReadFile);
                sidAbnormalErrorModel.getBufferedWriter().newLine();
                sidAbnormalErrorModel.getBufferedWriter().flush();
                CloseStream.closeWriteLocal(sidAbnormalErrorModel);
            }




            /*
             *通过path获取厂区 生产线 基本信息
             * */
            String[] pathStrArr = resultReadFile.replaceAll("hdfs://nameservice", "").split("/");
            if (pathStrArr.length < 9) {
                pathSizeLess9Model.getBufferedWriter().write(resultReadFile);
                pathSizeLess9Model.getBufferedWriter().newLine();
                pathSizeLess9Model.getBufferedWriter().flush();
                CloseStream.closeWriteLocal(pathSizeLess9Model);
                LOG.error("pathStrArr.length < 9 文件路径错误 !!" + resultReadFile);
                return;
            }

            String area = pathStrArr[2];
            String COB = pathStrArr[3];
            String EID = pathStrArr[4];
            String lot = pathStrArr[5];
            String unit = pathStrArr[7];
            String subpath = pathStrArr[6];
            String compositeParam = subpath.replaceAll("\\.|-+", "-");
            if ("GuCheng".equals(area) || "GuCheng2".equals(area) || "ChengBei".equals(area)) {
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
            if (vars.length < 4) {
                datepathSizeLess4Model.getBufferedWriter().write(resultReadFile + "  " + formatCompositeParam);
                datepathSizeLess4Model.getBufferedWriter().newLine();
                datepathSizeLess4Model.getBufferedWriter().flush();
                CloseStream.closeWriteLocal(datepathSizeLess4Model);
                LOG.error("vars.length<4 机台号时间文件路径错误 !!" + resultReadFile + "  " + formatCompositeParam);
                return;

            }
            String DEVICE_NUM = vars[0];
            String process_type = vars[1];
            String date_dir = vars[2].concat("-").concat(vars[3]);
            String working_shift = "";


            //机种带"-"的情况
            if ((vars.length == 6 && (formatCompositeParam.endsWith("D") || formatCompositeParam.endsWith("N") || formatCompositeParam.endsWith("d") || formatCompositeParam.endsWith("n")))
                    || (vars.length == 5 && !formatCompositeParam.endsWith("D") && !formatCompositeParam.endsWith("N") && !formatCompositeParam.endsWith("d") && !formatCompositeParam.endsWith("n"))) {
                process_type = vars[1].concat("-").concat(vars[2]);
                date_dir = vars[3].concat("-").concat(vars[4]);
                if (vars.length > 5) {
                    working_shift = vars[5];
                }
            } else if ((vars.length == 7 && (formatCompositeParam.endsWith("D") || formatCompositeParam.endsWith("N") || formatCompositeParam.endsWith("d") || formatCompositeParam.endsWith("n")))
                    || (vars.length == 6 && !formatCompositeParam.endsWith("D") && !formatCompositeParam.endsWith("N") && !formatCompositeParam.endsWith("d") && !formatCompositeParam.endsWith("n"))) {
                process_type = vars[1].concat("-").concat(vars[2]).concat("-").concat(vars[3]);
                date_dir = vars[4].concat("-").concat(vars[5]);
                if (vars.length > 6) {
                    working_shift = vars[6];
                }
            } else if (vars.length > 4) {
                working_shift = vars[4];
            }


            resultMap.put("area", area);
            resultMap.put("COB", COB);
            resultMap.put("EID", EID);
            resultMap.put("lot", lot);
            resultMap.put("unit", unit);
            resultMap.put("DEVICE_NUM", DEVICE_NUM);
            resultMap.put("process_type", process_type.replaceAll("o|O", "0"));
            resultMap.put("pathdate", date_dir + "-" + working_shift);
            resultMap.put("specialProcess", specialProcess);
            resultMap.put("working_shift", working_shift);
            resultMap.put("file", pathStrArr[8]);
            resultMap.put("subpath", subpath);
            /*
             * 路径值效验  长度效验 日期机种值效验
             * */
            if (area == null || StringUtils.isBlank(area) || EID == null || StringUtils.isBlank(EID) || COB == null || StringUtils.isBlank(COB) || DEVICE_NUM == null || StringUtils.isBlank(DEVICE_NUM)) {
                LOG.warn("parseLog() Job WAIN 异常  警告原因:area == null || StringUtils.isBlank(area) ||EID == null || StringUtils.isBlank(EID)  ||COB == null || StringUtils.isBlank(COB)||DEVICE_NUM == null || StringUtils.isBlank(DEVICE_NUM) \r\n area= " + area + "\r\n EID=" + EID + "\r\n COB=" + COB + "\r\n DEVICE_NUM=" + DEVICE_NUM + "\r\n" + resultReadFile);
                pathValueEmptyModel.getBufferedWriter().write(resultReadFile + "  area=" + area + "  EID=" + EID + "  COB=" + COB + "  DEVICE_NUM=" + DEVICE_NUM);
                pathValueEmptyModel.getBufferedWriter().newLine();
                pathValueEmptyModel.getBufferedWriter().flush();
                CloseStream.closeWriteLocal(pathValueEmptyModel);
            }
            if (vars.length < 3 || StringUtils.isBlank(process_type) || StringUtils.isBlank(date_dir) || !process_type.matches("^[a-zA-Z].*$") || !date_dir.matches("^[0-9].*$")) {
                LOG.warn("parseLog() Job WAIN 异常  警告原因:vars.length < 4 || StringUtils.isBlank(process_type) || StringUtils.isBlank(date_dir)|| !process_type.matches(\"^[a-zA-Z].*$\") || !date_dir.matches(\"^[0-9].*$\") \") \r\n vars.length= " + vars.length + "\r\n process_type=" + process_type + "\r\n date_dir=" + date_dir + "\r\n" + resultReadFile + "\r\n");
                pathValueNoMatchModel.getBufferedWriter().write(resultReadFile + "  vars.zzzzl= " + vars.length + "  process_type=" + process_type + "  date_dir=" + date_dir);
                pathValueNoMatchModel.getBufferedWriter().newLine();
                pathValueNoMatchModel.getBufferedWriter().flush();
                CloseStream.closeWriteLocal(pathValueNoMatchModel);
                //  SendEMailWarning.sendMail(RECEIVE_EMAIL, this.getClass().getName() + ".parseLog() Job WAIN", this.getClass().getName() + ".parseLog() Job WAIN 异常  警告原因:vars.length < 4 || StringUtils.isBlank(process_type) || StringUtils.isBlank(date_dir)|| !process_type.matches(\"^[a-zA-Z].*$\") || !date_dir.matches(\"^[0-9].*$\") \") \r\n vars.length= " + vars.length + "\r\n process_type=" + process_type + "\r\n date_dir=" + date_dir + "\r\n" + resultReadFile + "\r\n" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
            }

            /*
             * upsertSQL及column写出到文件
             * */
            String writeSql = RESULT_OUTPUT_LOCAL_DIR + "/upsertSQL_" + threadID + ".sql";

            //创建写入流
            WriteLocalModel sqlWriteLocalModel = operateFie.writeLocalFile(writeSql, true);
            String upsertSQL = parseLog.generateUpsertSQL(resultMap, area.toLowerCase());

            if (upsertSQL != null) {
                sqlWriteLocalModel.getBufferedWriter().write(upsertSQL);
                sqlWriteLocalModel.getBufferedWriter().newLine();
                sqlWriteLocalModel.getBufferedWriter().flush();
            }
            //log移入备份盘
            fileSystem.moveToLocalFile(new Path(numLogDir), new Path(bakpath));
            CloseStream.closeWriteLocal(contentErrorModel);
            CloseStream.closeWriteLocal(sqlWriteLocalModel);

        } catch (IOException e) {
            if (e.getMessage().contains("Cannot obtain block")) {
                operateFie.recoverLease(fileSystem, numLogDir, true);
            }
            LOG.error(".parseLog() 处理异常 ! " + resultReadFile + e.getMessage());
            e.printStackTrace();
        }
    }
}

