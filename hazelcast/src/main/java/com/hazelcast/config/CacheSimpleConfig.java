/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.config;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple configuration to hold parsed xml configuration.
 */
public class CacheSimpleConfig {

    /**
     * The number of minimum backup counter
     */
    public static final int MIN_BACKUP_COUNT = 0;
    /**
     * The number of maximum backup counter
     */
    public static final int MAX_BACKUP_COUNT = 6;
    /**
     * The number of default backup counter
     */
    public static final int DEFAULT_BACKUP_COUNT = 1;
    /**
     * Default InMemory Format.
     */
    public static final InMemoryFormat DEFAULT_IN_MEMORY_FORMAT = InMemoryFormat.BINARY;

    /**
     * Default Eviction Policy.
     */
    public static final EvictionPolicy DEFAULT_EVICTION_POLICY = EvictionPolicy.RANDOM;

    /**
     * The default value for percentage of max size
     */
    public static final int DEFAULT_MAX_MEMORY_SIZE_PERCENTAGE = 95;

    /**
     * Minimum eviction percentage
     */
    public static final  int MIN_EVICTION_PERCENTAGE = 0;

    /**
     * Default eviction percentage
     */
    public static final int DEFAULT_EVICTION_PERCENTAGE = 10;

    /**
     * Maximum eviction percentage
     */
    public static final int MAX_EVICTION_PERCENTAGE = 100;

    private String name;

    private String keyType;
    private String valueType;

    private boolean statisticsEnabled;
    private boolean managementEnabled;

    private boolean readThrough;
    private boolean writeThrough;

    private String cacheLoaderFactory;
    private String cacheWriterFactory;

    private String expiryPolicyFactory;
    private List<CacheSimpleEntryListenerConfig> cacheEntryListeners;

    private int asyncBackupCount = MIN_BACKUP_COUNT;
    private int backupCount = DEFAULT_BACKUP_COUNT;
    private InMemoryFormat inMemoryFormat = DEFAULT_IN_MEMORY_FORMAT;
    private EvictionPolicy evictionPolicy = DEFAULT_EVICTION_POLICY;
    // Default max size config, size = Integer.MAX_VALUE, policy = PER_NODE
    private MaxSizeConfig maxSizeConfig = new MaxSizeConfig();
    private int evictionPercentage;

    private CacheSimpleConfig readOnly;

    public CacheSimpleConfig(CacheSimpleConfig cacheSimpleConfig) {
        this.name = cacheSimpleConfig.name;
        this.keyType = cacheSimpleConfig.keyType;
        this.valueType = cacheSimpleConfig.valueType;
        this.statisticsEnabled = cacheSimpleConfig.statisticsEnabled;
        this.managementEnabled = cacheSimpleConfig.managementEnabled;
        this.readThrough = cacheSimpleConfig.readThrough;
        this.writeThrough = cacheSimpleConfig.writeThrough;
        this.cacheLoaderFactory = cacheSimpleConfig.cacheLoaderFactory;
        this.cacheWriterFactory = cacheSimpleConfig.cacheWriterFactory;
        this.expiryPolicyFactory = cacheSimpleConfig.expiryPolicyFactory;
        this.cacheEntryListeners = cacheSimpleConfig.cacheEntryListeners;
        this.asyncBackupCount = cacheSimpleConfig.asyncBackupCount;
        this.backupCount = cacheSimpleConfig.backupCount;
        this.inMemoryFormat = cacheSimpleConfig.inMemoryFormat;
        this.evictionPolicy = cacheSimpleConfig.evictionPolicy;
        this.maxSizeConfig = cacheSimpleConfig.maxSizeConfig;
        this.evictionPercentage = cacheSimpleConfig.evictionPercentage;
    }

    public CacheSimpleConfig() {

    }

    public CacheSimpleConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new CacheSimpleConfigReadOnly(this);
        }
        return readOnly;
    }

    public String getName() {
        return name;
    }

    public CacheSimpleConfig setName(String name) {
        this.name = name;
        return this;
    }

    public String getKeyType() {
        return keyType;
    }

    public CacheSimpleConfig setKeyType(String keyType) {
        this.keyType = keyType;
        return this;
    }

    public String getValueType() {
        return valueType;
    }

    public CacheSimpleConfig setValueType(String valueType) {
        this.valueType = valueType;
        return this;
    }

    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public CacheSimpleConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    public boolean isManagementEnabled() {
        return managementEnabled;
    }

    public CacheSimpleConfig setManagementEnabled(boolean managementEnabled) {
        this.managementEnabled = managementEnabled;
        return this;
    }

    public boolean isReadThrough() {
        return readThrough;
    }

    public CacheSimpleConfig setReadThrough(boolean readThrough) {
        this.readThrough = readThrough;
        return this;
    }

    public boolean isWriteThrough() {
        return writeThrough;
    }

    public CacheSimpleConfig setWriteThrough(boolean writeThrough) {
        this.writeThrough = writeThrough;
        return this;
    }

    public String getCacheLoaderFactory() {
        return cacheLoaderFactory;
    }

    public CacheSimpleConfig setCacheLoaderFactory(String cacheLoaderFactory) {
        this.cacheLoaderFactory = cacheLoaderFactory;
        return this;
    }

    public String getCacheWriterFactory() {
        return cacheWriterFactory;
    }

    public CacheSimpleConfig setCacheWriterFactory(String cacheWriterFactory) {
        this.cacheWriterFactory = cacheWriterFactory;
        return this;
    }

    public String getExpiryPolicyFactory() {
        return expiryPolicyFactory;
    }

    public CacheSimpleConfig setExpiryPolicyFactory(String expiryPolicyFactory) {
        this.expiryPolicyFactory = expiryPolicyFactory;
        return this;
    }

    public CacheSimpleConfig addEntryListenerConfig(CacheSimpleEntryListenerConfig listenerConfig) {
        getCacheEntryListeners().add(listenerConfig);
        return this;
    }

    public List<CacheSimpleEntryListenerConfig> getCacheEntryListeners() {
        if (cacheEntryListeners == null) {
            cacheEntryListeners = new ArrayList<CacheSimpleEntryListenerConfig>();
        }
        return cacheEntryListeners;
    }

    public CacheSimpleConfig setCacheEntryListeners(List<CacheSimpleEntryListenerConfig> cacheEntryListeners) {
        this.cacheEntryListeners = cacheEntryListeners;
        return this;
    }

    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    public CacheSimpleConfig setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = asyncBackupCount;
        return this;
    }

    public int getBackupCount() {
        return backupCount;
    }

    public CacheSimpleConfig setBackupCount(int backupCount) {
        this.backupCount = backupCount;
        return this;
    }

    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    public CacheSimpleConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        this.inMemoryFormat = inMemoryFormat;
        return this;
    }

    public EvictionPolicy getEvictionPolicy() {
        return evictionPolicy;
    }

    public CacheSimpleConfig setEvictionPolicy(EvictionPolicy evictionPolicy) {
        this.evictionPolicy = evictionPolicy;
        return this;
    }

    public MaxSizeConfig getMaxSizeConfig() {
        return maxSizeConfig;
    }

    public CacheSimpleConfig setMaxSizeConfig(MaxSizeConfig maxSizeConfig) {
        this.maxSizeConfig = maxSizeConfig;
        return this;
    }

    public int getEvictionPercentage() {
        return evictionPercentage;
    }

    public CacheSimpleConfig setEvictionPercentage(int evictionPercentage) {
        this.evictionPercentage = evictionPercentage;
        return this;
    }
}
