import cn.qtech.bigdata.core.parser.ParseLog;
import cn.qtech.bigdata.model.AtypiaProcess;
import cn.qtech.bigdata.model.RejectRateModel;
import cn.qtech.bigdata.model.bufferStream.ReadLocalModel;
import cn.qtech.bigdata.utils.OperateFie;
import com.alibaba.fastjson.JSONObject;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static cn.qtech.bigdata.comm.AppConstants.*;

public class Test2 {

    public static void main(String[] args) throws Exception {


        AtypiaProcess process = new AtypiaProcess();
        String file = "C:\\Users\\liqin.liu\\Desktop\\1150.log";
        ReadLocalModel readLocalModel = new OperateFie().readLocalFile(file);
        String line;
        JSONObject resultMap = new JSONObject(100);
        resultMap.put(SID_CNT, 1);
        RejectRateModel.columnList.stream().map(e -> resultMap.put(e.toUpperCase(), 0));
        resultMap.put("parse_time", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        ParseLog parseLog = new ParseLog();
        AtypiaProcess atypiaProcess = new AtypiaProcess();

        while ((line = readLocalModel.getBufferedReader().readLine()) != null) {

            String[] lineArr = line.split("\t");
            if (lineArr.length > 2 && lineArr[1] != null && lineArr[2] != null) {
                JSONObject parseResult = parseLog.parseData(lineArr, atypiaProcess, file);
                resultMap.putAll(parseResult);
            }
        }
        String sid = resultMap.getString("SID");
        //赋值MAIN_AA_START 优先级MAIN_AA_START->Pre_Test_Start->Onload_TIME
        String MAIN_AA_START = resultMap.getString("MAIN_AA_START");
        Integer gripperOpen_cnt = resultMap.getInteger(GRIPPEROPEN_CNT);
        System.out.println(sid +"        "+ gripperOpen_cnt);

        if (gripperOpen_cnt == 1) {
           // resultMap.put(SENSOR_THROW_TIGGER, "");
            System.out.println("gripperOpen_cnt == 1");
        }
        System.out.println(resultMap);
    }
}