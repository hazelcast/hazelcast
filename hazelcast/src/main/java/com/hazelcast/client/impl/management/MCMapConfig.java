package com.hazelcast.client.impl.management;

import com.hazelcast.client.impl.protocol.codec.MCGetMapConfigCodec.ResponseParameters;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy;

public final class MCMapConfig {
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
        return new MCMapConfig(
                InMemoryFormat.getById(params.inMemoryFormat),
                params.backupCount,
                params.asyncBackupCount,
                params.timeToLiveSeconds,
                params.maxIdleSeconds,
                params.maxSize,
                MaxSizePolicy.getById(params.maxSizePolicy),
                params.readBackupData,
                EvictionPolicy.getById(params.evictionPolicy),
                params.mergePolicy
        );
    }

    public MCMapConfig(InMemoryFormat inMemoryFormat, int backupCount, int asyncBackupCount,
                       int timeToLiveSeconds, int maxIdleSeconds, int maxSize,
                       MaxSizePolicy maxSizePolicy, boolean readBackupData, EvictionPolicy evictionPolicy,
                       String mergePolicy) {
        this.inMemoryFormat = inMemoryFormat;
        this.backupCount = backupCount;
        this.asyncBackupCount = asyncBackupCount;
        this.timeToLiveSeconds = timeToLiveSeconds;
        this.maxIdleSeconds = maxIdleSeconds;
        this.maxSize = maxSize;
        this.maxSizePolicy = maxSizePolicy;
        this.readBackupData = readBackupData;
        this.evictionPolicy = evictionPolicy;
        this.mergePolicy = mergePolicy;
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
