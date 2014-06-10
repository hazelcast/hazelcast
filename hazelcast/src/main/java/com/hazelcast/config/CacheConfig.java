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


import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.MutableConfiguration;

import static com.hazelcast.util.ValidationUtil.isNotNull;

public class CacheConfig<K,V> extends MutableConfiguration<K,V> {

    public final static int MIN_BACKUP_COUNT = 0;
    public final static int DEFAULT_BACKUP_COUNT = 1;
    public final static int MAX_BACKUP_COUNT = 6;
//    public final static int MIN_EVICTION_PERCENTAGE = 0;
//    public final static int DEFAULT_EVICTION_PERCENTAGE = 20;
//    public final static int DEFAULT_EVICTION_THRESHOLD_PERCENTAGE = 95;
//    public final static int MAX_EVICTION_PERCENTAGE = 100;
//    public final static int DEFAULT_TTL_SECONDS = 0;
    public final static EvictionPolicy DEFAULT_EVICTION_POLICY = EvictionPolicy.RANDOM;

    public final static InMemoryFormat DEFAULT_IN_MEMORY_FORMAT = InMemoryFormat.BINARY;

    private String name = null;

    private int backupCount = DEFAULT_BACKUP_COUNT;

    private int asyncBackupCount = MIN_BACKUP_COUNT;

    private EvictionPolicy evictionPolicy = DEFAULT_EVICTION_POLICY;

    private InMemoryFormat inMemoryFormat = DEFAULT_IN_MEMORY_FORMAT;

    private NearCacheConfig nearCacheConfig;

    private CacheConfigReadOnly<K,V> readOnly;

    public CacheConfig() {
    }

    public CacheConfig(CompleteConfiguration<K,V> completeConfiguration) {
        super(completeConfiguration);
        if(completeConfiguration instanceof CacheConfig){
            final CacheConfig config = (CacheConfig)completeConfiguration;
//        this.name = config.name;
            this.backupCount = config.backupCount;
            this.asyncBackupCount = config.asyncBackupCount;
//        this.evictionPercentage = config.evictionPercentage;
//        this.evictionThresholdPercentage = config.evictionThresholdPercentage;
//        this.timeToLiveSeconds = config.timeToLiveSeconds;
            this.evictionPolicy = config.evictionPolicy;
            this.isStatisticsEnabled = config.isStatisticsEnabled;
            if (config.nearCacheConfig != null) {
                this.nearCacheConfig = new NearCacheConfig(config.nearCacheConfig);
            }
        }
    }

    public CacheConfigReadOnly<K,V> getAsReadOnly(){
        if (readOnly == null){
            readOnly = new CacheConfigReadOnly<K,V>(this);
        }
        return readOnly;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public CacheConfig<K,V> setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * @return the backupCount
     * @see #getAsyncBackupCount()
     */
    public int getBackupCount() {
        return backupCount;
    }

    /**
     * Number of synchronous backups. If 1 is set as the backup-count for example,
     * then all entries of the map will be copied to another JVM for
     * fail-safety. 0 means no sync backup.
     *
     * @param backupCount the backupCount to set
     * @see #setAsyncBackupCount(int)
     */
    public CacheConfig<K,V> setBackupCount(final int backupCount) {
        if (backupCount < MIN_BACKUP_COUNT) {
            throw new IllegalArgumentException("map backup count must be equal to or bigger than "
                    + MIN_BACKUP_COUNT);
        }
        if ((backupCount + this.asyncBackupCount) > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("total (sync + async) map backup count must be less than "
                    + MAX_BACKUP_COUNT);
        }
        this.backupCount = backupCount;
        return this;
    }

    /**
     * @return the asyncBackupCount
     * @see #setBackupCount(int)
     */
    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    /**
     * Number of asynchronous backups.
     * 0 means no backup.
     *
     * @param asyncBackupCount the asyncBackupCount to set
     * @see #setBackupCount(int)
     */
    public CacheConfig<K,V> setAsyncBackupCount(final int asyncBackupCount) {
        if (asyncBackupCount < MIN_BACKUP_COUNT) {
            throw new IllegalArgumentException("map async backup count must be equal to or bigger than "
                    + MIN_BACKUP_COUNT);
        }
        if ((this.backupCount + asyncBackupCount) > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("total (sync + async) map backup count must be less than "
                    + MAX_BACKUP_COUNT);
        }
        this.asyncBackupCount = asyncBackupCount;
        return this;
    }

    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    /**
     * @return the evictionPolicy
     */
    public EvictionPolicy getEvictionPolicy() {
        return evictionPolicy;
    }

    /**
     * @param evictionPolicy the evictionPolicy to set
     */
    public CacheConfig<K,V> setEvictionPolicy(EvictionPolicy evictionPolicy) {
        this.evictionPolicy = evictionPolicy;
        return this;
    }


    /**
     * @return data type that will be used for storing records.
     */
    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    /**
     * Data type that will be used for storing records.
     * Possible values:
     * BINARY (default): keys and values will be stored as binary data
     * OBJECT : values will be stored in their object forms
     *
     * @param inMemoryFormat the record type to set
     * @throws IllegalArgumentException if inMemoryFormat is null.
     */
    public CacheConfig<K,V> setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        this.inMemoryFormat = isNotNull(inMemoryFormat,"inMemoryFormat");
        return this;
    }
//    @Override
//    public Factory<ExpiryPolicy> getExpiryPolicyFactory() {
//        return super.getExpiryPolicyFactory();
//    }
}
