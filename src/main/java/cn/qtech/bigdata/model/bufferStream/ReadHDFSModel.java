package cn.qtech.bigdata.model.bufferStream;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import lombok.*;

@Setter
@Getter
@ToString
public class ReadHDFSModel {
    FSDataInputStream fSDataInputStream;
    InputStreamReader inputStreamReader;
    BufferedReader bufferedReader;


}
