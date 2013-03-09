package com.hazelcast.util;

public class PartitionKeyUtil {

    public static String getBaseName(String name){
        if(name == null)return null;
        int indexOf = name.indexOf('@');
        if(indexOf == -1) return name;
        return name.substring(0,indexOf);
    }

    public static Object getPartitionKey(Object key) {
        if (key == null) return null;
        if (!(key instanceof String)) return key;

        String s = (String) key;
        int firstIndexOf = s.indexOf('@');
        if (firstIndexOf > -1) {
            key = s.substring(firstIndexOf + 1);
        }

        return key;
    }
}
