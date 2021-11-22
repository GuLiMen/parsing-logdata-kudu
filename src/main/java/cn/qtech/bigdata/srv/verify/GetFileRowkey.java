package cn.qtech.bigdata.srv.verify;

import cn.qtech.bigdata.model.bufferStream.ReadLocalModel;
import cn.qtech.bigdata.model.bufferStream.WriteLocalModel;
import cn.qtech.bigdata.utils.CloseStream;
import cn.qtech.bigdata.utils.ConnectPhoenixDB;
import cn.qtech.bigdata.utils.OperateFie;
import cn.qtech.bigdata.utils.PhoenixJDBC;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;


public class GetFileRowkey {
    private static String resultReadFile;
    private static String rootPath;
    private static String numLogDir;
    private static File fullFile;

    public static void main(String[] args) throws IOException {
        rootPath = System.getProperty("rootPath");
        numLogDir = System.getProperty("numLogDir");
        fullFile = new File(rootPath + numLogDir);

        boolean opreate = getLogFile();
        getRowkey(opreate);


    }

    private static boolean getLogFile() throws IOException {
        ArrayList<String> pathList = new ArrayList<>();
        if (fullFile == null || !fullFile.exists()) {
            System.out.println("fullFile==null || !fullFile.exists() fullFile=" + fullFile.getAbsolutePath());
            return false;
        }

        for (File flumeFile : fullFile.listFiles()) {
            if (flumeFile.getName().startsWith(".")) {
                continue;
            }
            pathList.add(flumeFile.getAbsolutePath());
        }

        if (pathList.size() == 0) {
            System.out.println("没有文件" + fullFile.getAbsolutePath());
            return false;
        } else if (pathList.size() == 1) {
            resultReadFile = pathList.get(0);
            return true;
        }

        for (String f : pathList) {
            if (f.contains("FlumeDataTotal")) {
                resultReadFile = f;
                return true;
            }
        }


        //  System.out.println("Files not merged:" + fullFile.getAbsolutePath());
        return false;
    }


    private static void getRowkey(boolean opreate) throws IOException {
        if (!opreate) {
            return;
        }
        OperateFie operateFie = new OperateFie();
        // System.out.println(resultReadFile);
        ReadLocalModel parsereadLocalModel = operateFie.readLocalFile(resultReadFile);
        String line;
        boolean flag1 = false;
        boolean flag2 = false;
        boolean flag3 = false;
        HashMap<String, String> resultMap = new HashMap<>();
        while ((line = parsereadLocalModel.getBufferedReader().readLine()) != null) {
            String[] lineArr = line.split("\t");

            if (line.startsWith("STATION\tOnload\tLensTray")) {
                System.out.println("LensTray:" +line+ numLogDir);
                return;
            }
            if (line.startsWith("STATION\tOnload\t") && lineArr.length ==4) {
                if (!line.startsWith("STATION\tOnload\tSensorTray")) {
                    System.out.println("不是 SensorTray:" +line+ numLogDir);
                    return;
                }

            }

            if (lineArr.length > 2 && lineArr[1] != null && lineArr[2] != null) {
                String upperx2 = lineArr[2].toUpperCase();
                String upperx1 = lineArr[1].toUpperCase();

                if (("SID".toUpperCase().equals(upperx2) || "SenserID".toUpperCase().equals(upperx2) || "RecordSensorID".toUpperCase().equals(upperx2) || "ReadSensorID".toUpperCase().equals(upperx2)) && lineArr[3] != null && lineArr.length < 6) {
                    if ("RESULT".equals(lineArr[3].toUpperCase())) {
                        resultMap.put("SID", lineArr[4]);
                        flag1 = true;
                    }

                } else if ("Main_Test".toUpperCase().equals(upperx1) && "Main AA Start".toUpperCase().equals(upperx2)) {
                    resultMap.put(upperx2.replaceAll("\\s+", "_"), lineArr[3]);
                    flag2 = true;
                } else if ("Pre_Test".toUpperCase().equals(upperx1) && "Init".toUpperCase().equals(upperx2)) {
                    String upper3 = lineArr[3].toUpperCase();
                    if ("INIT".equals(upper3)) {
                        resultMap.put(upperx1.concat("_").concat(upper3), lineArr[4]);
                    }
                    flag3 = true;
                }

         /*       if (flag1 && flag2 && flag3) {
                    break;
                }*/
            }
        }

        String sid = resultMap.get("SID");
        String MAIN_AA_START = resultMap.get("MAIN_AA_START");
        WriteLocalModel sidEmptyModel = operateFie.writeLocalFile("/data/workspace/alarm/verifyDataCount/sidEmpty", true);
        WriteLocalModel MAIN_AA_START_EmptyModel = operateFie.writeLocalFile("/data/workspace/alarm/verifyDataCount/MAIN_AA_START_Empty", true);
        WriteLocalModel verifyModel = operateFie.writeLocalFile("/data/workspace/alarm/verifyDataCount/verifyTempData", true);
        WriteLocalModel sidEmptyAndInitFailModel = operateFie.writeLocalFile("/data/workspace/alarm/verifyDataCount/sidEmptyAndInitFail", true);

        String rowValue = sid;
        if (MAIN_AA_START != null && sid != null && !StringUtils.isBlank(sid)) {
            rowValue = new StringBuilder(MAIN_AA_START.replaceAll("\\s+|-|:", "")).reverse().toString().substring(0, 8).concat("_").concat(sid);
        } else if (sid == null || StringUtils.isBlank(sid)) {
            sidEmptyModel.getBufferedWriter().write(numLogDir);
            sidEmptyModel.getBufferedWriter().newLine();
            sidEmptyModel.getBufferedWriter().flush();
            if (resultMap.get("PRE_TEST_INIT") == null || "fail".equals(resultMap.get("PRE_TEST_INIT").toLowerCase())) {
                sidEmptyAndInitFailModel.getBufferedWriter().write(numLogDir);
                sidEmptyAndInitFailModel.getBufferedWriter().newLine();
                sidEmptyAndInitFailModel.getBufferedWriter().flush();
                return;
            }
            sid = (int) (Math.random() * 10)+"" + (int) (Math.random() * 10)+"" + (int) (Math.random() * 10) + "_rd" + System.currentTimeMillis() + "";
            rowValue = sid;
        }

        if (MAIN_AA_START == null || StringUtils.isBlank(MAIN_AA_START)) {
            MAIN_AA_START_EmptyModel.getBufferedWriter().write(numLogDir);
            MAIN_AA_START_EmptyModel.getBufferedWriter().newLine();
            MAIN_AA_START_EmptyModel.getBufferedWriter().flush();
            MAIN_AA_START="";
        }

        verifyModel.getBufferedWriter().write(rowValue + "," + numLogDir+","+MAIN_AA_START);
        verifyModel.getBufferedWriter().newLine();
        verifyModel.getBufferedWriter().flush();



        CloseStream.closeReadLocal(parsereadLocalModel);
        CloseStream.closeWriteLocal(sidEmptyAndInitFailModel);
        CloseStream.closeWriteLocal(verifyModel);
        CloseStream.closeWriteLocal(sidEmptyModel);
        CloseStream.closeWriteLocal(MAIN_AA_START_EmptyModel);

    }

}
