import cn.qtech.bigdata.model.AtypiaProcess;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;

import javax.xml.bind.SchemaOutputResolver;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static cn.qtech.bigdata.comm.AppConstants.SENSOR_THROW_TIGGER;

public class test3 {
    public static final ArrayList<Integer> pool = new ArrayList<>();
    public static final int data[] = new int[11]; /*开辟了一个长度为3的数组*/

    public static void main(String[] args) {
        AtypiaProcess atypiaProcess = new AtypiaProcess();
        System.out.println(atypiaProcess+"0000000000000000");
        atypiaProcess.setAlignment(true);
        System.out.println(atypiaProcess+"0000000000000000");

        String s = new Integer(0).toString();
        System.out.println(s);
        JSONObject resultMap = new JSONObject();

        resultMap.put("parse_time", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        System.out.println(resultMap.get("parse_time"));
        resultMap.put(SENSOR_THROW_TIGGER, "VCM_Move");
        Object o = resultMap.get(SENSOR_THROW_TIGGER);
        if(o==null){
            System.out.println("***********************");
        }
        if(resultMap.get(SENSOR_THROW_TIGGER).equals("VCM_Move")){
            System.out.println("------------0000000000000--------");
            resultMap.put(SENSOR_THROW_TIGGER,"VCM_Move2");
        }
        System.out.println(resultMap.get(SENSOR_THROW_TIGGER)+"========");
        System.out.println(resultMap.getString(SENSOR_THROW_TIGGER).toLowerCase()+"------");

        String MAIN_AA_START="";
        MAIN_AA_START= MAIN_AA_START == null || StringUtils.isBlank(MAIN_AA_START) ? resultMap.getString("Onload_TIME") : MAIN_AA_START;

        System.out.println(MAIN_AA_START);

        /*
         * 统一check ChartAlignment 名称
         * */
        if (resultMap.containsKey("MTF_CHECK1_RES_1")) {
            //移除check1 命名统一为check check2
            Object CHECK1_RES_1 = resultMap.getOrDefault("MTF_CHECK1_RES_1", "");
            Object CHECK1_RES_2 = resultMap.getOrDefault("MTF_CHECK1_RES_2", "");
            Object CHECK1_RES_3 = resultMap.getOrDefault("MTF_CHECK1_RES_3", "");
            Object CHECK1_RES_4 = resultMap.getOrDefault("MTF_CHECK1_RES_4", "");
            resultMap.remove("MTF_CHECK1_RES_1");
            resultMap.remove("MTF_CHECK1_RES_2");
            resultMap.remove("MTF_CHECK1_RES_3");
            resultMap.remove("MTF_CHECK1_RES_4");

            if (resultMap.containsKey("MTF_CHECK_RES_1") && !resultMap.containsKey("MTF_CHECK2_RES_1")) {
                //check1值给check2
                resultMap.put("MTF_CHECK2_RES_1", CHECK1_RES_1);
                resultMap.put("MTF_CHECK2_RES_2", CHECK1_RES_2);
                resultMap.put("MTF_CHECK2_RES_3", CHECK1_RES_3);
                resultMap.put("MTF_CHECK2_RES_4", CHECK1_RES_4);
            } else if (!resultMap.containsKey("MTF_CHECK_RES_1")) {
                //check1值给check
                resultMap.put("MTF_CHECK_RES_1", CHECK1_RES_1);
                resultMap.put("MTF_CHECK_RES_2", CHECK1_RES_2);
                resultMap.put("MTF_CHECK_RES_3", CHECK1_RES_3);
                resultMap.put("MTF_CHECK_RES_4", CHECK1_RES_4);
            } else if (resultMap.containsKey("MTF_CHECK_RES_1") && resultMap.containsKey("MTF_CHECK2_RES_1")) {
               // LOG.error("文件同时存在MTF_CHECK MTF_CHECK1 MTF_CHECK2" + numLogDir);
            }
        }

        if (resultMap.containsKey("ChartAlignment1_1")) {
            //移除check1 命名统一为check check2
            Object alignment1_1 = resultMap.getOrDefault("ChartAlignment1_1", "");
            Object alignment1_2 = resultMap.getOrDefault("ChartAlignment1_2", "");
            Object alignment1_3 = resultMap.getOrDefault("ChartAlignment1_3", "");
            Object alignment1_4 = resultMap.getOrDefault("ChartAlignment1_4", "");
            resultMap.remove("ChartAlignment1_1");
            resultMap.remove("ChartAlignment1_2");
            resultMap.remove("ChartAlignment1_3");
            resultMap.remove("ChartAlignment1_4");

            if (resultMap.containsKey("ChartAlignment_1") && !resultMap.containsKey("ChartAlignment2_1")) {
                //ChartAlignment1值给ChartAlignment2
                resultMap.put("ChartAlignment2_1", alignment1_1);
                resultMap.put("ChartAlignment2_2", alignment1_2);
                resultMap.put("ChartAlignment2_3", alignment1_3);
                resultMap.put("ChartAlignment2_4", alignment1_4);
            } else if (!resultMap.containsKey("ChartAlignment_1")) {
                //ChartAlignment1值给ChartAlignment
                resultMap.put("ChartAlignment_1", alignment1_1);
                resultMap.put("ChartAlignment_2", alignment1_2);
                resultMap.put("ChartAlignment_3", alignment1_3);
                resultMap.put("ChartAlignment_4", alignment1_4);
            } else if (resultMap.containsKey("ChartAlignment_1") && resultMap.containsKey("ChartAlignment2_1")) {
               // LOG.error("文件同时存在ChartAlignment ChartAlignment1 ChartAlignment2" + numLogDir);
            }
        }

    }


}
