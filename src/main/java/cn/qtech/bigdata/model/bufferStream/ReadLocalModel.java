package cn.qtech.bigdata.model.bufferStream;

import lombok.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;


@Getter
@Setter
@ToString
public class ReadLocalModel {
    FileInputStream fileInputStream;
    InputStreamReader inputStreamReader;
    BufferedReader bufferedReader;
}
