package cn.qtech.bigdata.model.bufferStream;

import lombok.*;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

@Setter
@Getter
@ToString
public class WriteLocalModel {
    FileOutputStream fileOutputStream;
    OutputStreamWriter outputStreamWriter;

    BufferedWriter bufferedWriter;


}
