package cn.qtech.bigdata.core.batch;


import cn.qtech.bigdata.model.RecordResponse;

import java.io.IOException;


public abstract class ABatchEngineNew {

    /**
     * 获取解析文件
     *
     * @param
     * @return 文件是否可操作:是否达到时间条件...
     * @throws IOException
     */
    abstract RecordResponse getLogFile(String numLogDir) throws IOException;

    /**
     * 解析日志
     * @param
     * @throws IOException
     */
    abstract void parseLog(RecordResponse recordResponse) throws IOException;


    public void start(String numLogDir) {

        try {
            RecordResponse response = this.getLogFile(numLogDir);
            this.parseLog(response);

        } catch (IOException e) {
            e.printStackTrace();
        }


    }


}
