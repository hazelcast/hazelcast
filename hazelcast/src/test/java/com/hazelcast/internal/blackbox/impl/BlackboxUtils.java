package com.hazelcast.internal.blackbox.impl;

import java.util.HashMap;
import java.util.Map;

public class BlackboxUtils {

    public static Map createMap(int size){
        Map map = new HashMap();
        for(int k=0;k<size;k++){
            map.put(k,k);
        }
        return map;
    }
}
