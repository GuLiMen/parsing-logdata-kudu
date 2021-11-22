package cn.qtech.bigdata.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.fs.FileSystem;


@Getter
@Setter
public class RecordResponse {
    boolean Operability;
    String resultReadFile;
    String numLogDir;
    String bakpath;

/*    public boolean getOperability() {
        return operability;
    }*/
}
