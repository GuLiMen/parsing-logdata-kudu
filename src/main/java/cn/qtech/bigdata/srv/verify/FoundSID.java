package cn.qtech.bigdata.srv.verify;

import cn.qtech.bigdata.model.bufferStream.ReadLocalModel;
import cn.qtech.bigdata.utils.OperateFie;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class FoundSID {
    public static void main(String[] args) throws IOException {
        OperateFie operateFie = new OperateFie();
        String date_dir = "/aalogfile/flume/GuCheng/COB1/EQ01000003300158/Lot/7-2-C08F17-11-27-D";
        ReadLocalModel parsereadLocalModel = operateFie.readLocalFile(date_dir);
        String line;
        boolean flag1 = false;
        boolean flag2 = false;
        HashMap<String, String> resultMap = new HashMap<>();
         AtomicInteger i = new AtomicInteger(0);
        while ((line = parsereadLocalModel.getBufferedReader().readLine()) != null) {
            if (line.startsWith("STATION\tOnload\tLensTray")) {

                return;
            }
            String[] lineArr = line.split("\t");

            if (lineArr.length > 2 && lineArr[1] != null && lineArr[2] != null) {
                String upperx2 = lineArr[2].toUpperCase();
                String upperx1 = lineArr[1].toUpperCase();

                if (("SID".toUpperCase().equals(upperx2) || "SenserID".toUpperCase().equals(upperx2) || "RecordSensorID".toUpperCase().equals(upperx2) || "ReadSensorID".toUpperCase().equals(upperx2)) && lineArr[3] != null && lineArr.length < 6) {
                    if ("RESULT".equals(lineArr[3].toUpperCase())) {
                        i.getAndIncrement();

                        resultMap.put("SID", lineArr[4]);
                        flag1 = true;
                    }

                }
                if (flag1) {
                    break;
                }
            }
        }
        System.out.println(i.get());

    }
}
