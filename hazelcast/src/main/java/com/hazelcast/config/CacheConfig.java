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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import java.io.IOException;

import static com.hazelcast.util.ValidationUtil.isNotNull;

/**
 * Contains all the configuration for the {@link com.hazelcast.cache.ICache}
 *
 * @param <K>
 * @param <V>
 */
public class CacheConfig<K, V> extends AbstractCacheConfig<K, V> {

    private int asyncBackupCount = MIN_BACKUP_COUNT;
    private int backupCount = DEFAULT_BACKUP_COUNT;
    private InMemoryFormat inMemoryFormat = DEFAULT_IN_MEMORY_FORMAT;
    private EvictionPolicy evictionPolicy = DEFAULT_EVICTION_POLICY;
    private String name;
    private String managerPrefix;
    private String uriString;
    private NearCacheConfig nearCacheConfig;

    public CacheConfig() {
        super();
    }

    public CacheConfig(CompleteConfiguration<K, V> configuration) {
        super(configuration);
        if (configuration instanceof CacheConfig) {
            final CacheConfig config = (CacheConfig) configuration;
            this.backupCount = config.backupCount;
            this.asyncBackupCount = config.asyncBackupCount;
            this.evictionPolicy = config.evictionPolicy;
            if (config.nearCacheConfig != null) {
                this.nearCacheConfig = new NearCacheConfig(config.nearCacheConfig);
            }
        }
    }

    /**
     * Immutable CacheConfig instance
     */
    public CacheConfigReadOnly<K, V> getAsReadOnly() {
        return new CacheConfigReadOnly<K, V>(this);
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
    public CacheConfig<K, V> setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * @return the managerPrefix
     */
    public String getManagerPrefix() {
        return managerPrefix;
    }

    public CacheConfig<K, V> setManagerPrefix(String managerPrefix) {
        this.managerPrefix = managerPrefix;
        return this;
    }

    public String getUriString() {
        return uriString;
    }

    public CacheConfig<K, V> setUriString(String uriString) {
        this.uriString = uriString;
        return this;
    }

    /**
     * @return the name with manager scope prefix
     */
    public String getNameWithPrefix() {
        return managerPrefix + name;
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
    public CacheConfig<K, V> setBackupCount(final int backupCount) {
        if (backupCount < MIN_BACKUP_COUNT) {
            throw new IllegalArgumentException("map backup count must be equal to or bigger than " + MIN_BACKUP_COUNT);
        }
        if ((backupCount + this.asyncBackupCount) > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("total (sync + async) map backup count must be less than " + MAX_BACKUP_COUNT);
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
    public CacheConfig<K, V> setAsyncBackupCount(final int asyncBackupCount) {
        if (asyncBackupCount < MIN_BACKUP_COUNT) {
            throw new IllegalArgumentException("map async backup count must be equal to or bigger than " + MIN_BACKUP_COUNT);
        }
        if ((this.backupCount + asyncBackupCount) > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("total (sync + async) map backup count must be less than " + MAX_BACKUP_COUNT);
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
    public CacheConfig<K, V> setEvictionPolicy(EvictionPolicy evictionPolicy) {
        this.evictionPolicy = evictionPolicy;
        return this;
    }

    public NearCacheConfig getNearCacheConfig() {
        return nearCacheConfig;
    }

    public CacheConfig setNearCacheConfig(NearCacheConfig nearCacheConfig) {
        this.nearCacheConfig = nearCacheConfig;
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
    public CacheConfig<K, V> setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        this.inMemoryFormat = isNotNull(inMemoryFormat, "inMemoryFormat");
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(name);
        out.writeUTF(managerPrefix);
        out.writeUTF(uriString);
        out.writeInt(backupCount);
        out.writeInt(asyncBackupCount);

        out.writeInt(inMemoryFormat.ordinal());
        out.writeInt(evictionPolicy.ordinal());

        out.writeObject(nearCacheConfig);

        //SUPER
        out.writeObject(keyType);
        out.writeObject(valueType);
        out.writeObject(cacheLoaderFactory);
        out.writeObject(cacheWriterFactory);
        out.writeObject(expiryPolicyFactory);

        out.writeBoolean(isReadThrough);
        out.writeBoolean(isWriteThrough);
        out.writeBoolean(isStoreByValue);
        out.writeBoolean(isManagementEnabled);
        out.writeBoolean(isStatisticsEnabled);

        final boolean listNotEmpty = listenerConfigurations != null && !listenerConfigurations.isEmpty();
        out.writeBoolean(listNotEmpty);
        if (listNotEmpty) {
            out.writeInt(listenerConfigurations.size());
            for (CacheEntryListenerConfiguration<K, V> cc : listenerConfigurations) {
                out.writeObject(cc);
            }
        }

    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        name = in.readUTF();
        managerPrefix = in.readUTF();
        uriString = in.readUTF();
        backupCount = in.readInt();
        asyncBackupCount = in.readInt();

        final int resultInMemoryFormat = in.readInt();
        inMemoryFormat = InMemoryFormat.values()[resultInMemoryFormat];

        final int resultEvictionPolicy = in.readInt();
        evictionPolicy = EvictionPolicy.values()[resultEvictionPolicy];

        nearCacheConfig = in.readObject();

        //SUPER
        keyType = in.readObject();
        valueType = in.readObject();
        cacheLoaderFactory = in.readObject();
        cacheWriterFactory = in.readObject();
        expiryPolicyFactory = in.readObject();

        isReadThrough = in.readBoolean();
        isWriteThrough = in.readBoolean();
        isStoreByValue = in.readBoolean();
        isManagementEnabled = in.readBoolean();
        isStatisticsEnabled = in.readBoolean();

        final boolean listNotEmpty = in.readBoolean();
        if (listNotEmpty) {
            final int size = in.readInt();
            listenerConfigurations = createConcurrentSet();
            for (int i = 0; i < size; i++) {
                listenerConfigurations.add((CacheEntryListenerConfiguration<K, V>) in.readObject());
            }
        }

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (managerPrefix != null ? managerPrefix.hashCode() : 0);
        result = 31 * result + (uriString != null ? uriString.hashCode() : 0);
        return result;
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final CacheConfig that = (CacheConfig) o;


        if (managerPrefix != null ? !managerPrefix.equals(that.managerPrefix) : that.managerPrefix != null) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (uriString != null ? !uriString.equals(that.uriString) : that.uriString != null) {
            return false;
        }

        return super.equals(o);
    }

}
