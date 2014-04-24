package com.hazelcast.util;

import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import java.util.concurrent.atomic.AtomicLong;

public class JsonUtil {

    public static JsonValue getAtomicLongAsJSON(AtomicLong value){
        JsonObject atomicLong = new JsonObject();
        atomicLong.add("value", value.get());
        return  atomicLong;
    }
}
