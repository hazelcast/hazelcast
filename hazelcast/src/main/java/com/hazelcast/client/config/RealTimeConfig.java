package com.hazelcast.client.config;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class RealTimeConfig {
    private final HashMap<String, Long> limits = new HashMap();

    public Long getMapLimit(String mapName) {
        return limits.get(mapName);
    }

    public void setMapLimit(String mapName, long limit, TimeUnit unit) {
        limits.put(mapName, TimeUnit.NANOSECONDS.convert(limit, unit));
    }
}
