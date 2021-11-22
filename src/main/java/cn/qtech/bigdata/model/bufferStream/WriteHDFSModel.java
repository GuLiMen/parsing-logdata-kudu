package cn.qtech.bigdata.model.bufferStream;

import lombok.*;

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.Objects;

@Getter
@Setter
@ToString
public class WriteHDFSModel {
    FSDataOutputStream fsDataOutputStream;
    OutputStreamWriter outputStreamWriter;
    BufferedWriter bufferedWriter;


}
