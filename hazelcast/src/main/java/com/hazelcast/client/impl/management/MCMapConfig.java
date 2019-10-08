package com.hazelcast.client.impl.management;

import com.hazelcast.client.impl.protocol.codec.MCGetMapConfigCodec.ResponseParameters;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy;

public class MCMapConfig {
    private InMemoryFormat inMemoryFormat;
    private int backupCount;
    private int asyncBackupCount;
    private int timeToLiveSeconds;
    private int maxIdleSeconds;
    private int maxSize;
    private MaxSizePolicy maxSizePolicy;
    private boolean readBackupData;
    private EvictionPolicy evictionPolicy;
    private String mergePolicy;

    public static MCMapConfig fromResponse(ResponseParameters params) {
        MCMapConfig config = new MCMapConfig();
        config.inMemoryFormat = InMemoryFormat.getById(params.inMemoryFormat);
        config.backupCount = params.backupCount;
        config.asyncBackupCount = params.asyncBackupCount;
        config.timeToLiveSeconds = params.timeToLiveSeconds;
        config.maxIdleSeconds = params.maxIdleSeconds;
        config.maxSize = params.maxSize;
        config.maxSizePolicy = MaxSizePolicy.getById(params.maxSizePolicy);
        config.readBackupData = params.readBackupData;
        config.evictionPolicy = EvictionPolicy.getById(params.evictionPolicy);
        config.mergePolicy = params.mergePolicy;
        return config;
    }

    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    public int getBackupCount() {
        return backupCount;
    }

    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    public int getMaxIdleSeconds() {
        return maxIdleSeconds;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public MaxSizePolicy getMaxSizePolicy() {
        return maxSizePolicy;
    }

    public boolean isReadBackupData() {
        return readBackupData;
    }

    public EvictionPolicy getEvictionPolicy() {
        return evictionPolicy;
    }

    public String getMergePolicy() {
        return mergePolicy;
    }
}
