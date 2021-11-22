package cn.qtech.bigdata.utils;

import com.alibaba.fastjson.JSONObject;

import java.util.*;

public class SortField {
    public static List<String> sort(HashSet set) {
        ArrayList arrayList = new ArrayList<String>(set);
        Collections.sort(arrayList) ;
        return arrayList;
    }
}
