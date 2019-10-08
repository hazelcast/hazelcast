package com.hazelcast.client.impl.management;

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MaxSizeConfig;

public class UpdateMapConfigParameters {
    private final String map;
    private final int timeToLiveSeconds;
    private final int maxIdleSeconds;
    private final EvictionPolicy evictionPolicy;
    private final boolean readBackupData;
    private final int maxSize;
    private final MaxSizeConfig.MaxSizePolicy maxSizePolicy;

    /**
     */
    public UpdateMapConfigParameters(String map, int timeToLiveSeconds, int maxIdleSeconds, EvictionPolicy evictionPolicy, boolean readBackupData, int maxSize, MaxSizeConfig.MaxSizePolicy maxSizePolicy) {
        this.map = map;
        this.timeToLiveSeconds = timeToLiveSeconds;
        this.maxIdleSeconds = maxIdleSeconds;
        this.evictionPolicy = evictionPolicy;
        this.readBackupData = readBackupData;
        this.maxSize = maxSize;
        this.maxSizePolicy = maxSizePolicy;
    }

    public String getMap() {
        return map;
    }

    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    public int getMaxIdleSeconds() {
        return maxIdleSeconds;
    }

    public EvictionPolicy getEvictionPolicy() {
        return evictionPolicy;
    }

    public boolean isReadBackupData() {
        return readBackupData;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public MaxSizeConfig.MaxSizePolicy getMaxSizePolicy() {
        return maxSizePolicy;
    }
}
