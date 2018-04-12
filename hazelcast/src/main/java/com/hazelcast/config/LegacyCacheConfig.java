/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.nio.serialization.TypedDataSerializable;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import java.io.IOException;
import java.util.Set;

/**
 * Contains all the configuration for the {@link com.hazelcast.cache.ICache} (used for backward compatibility).
 * <p>
 * This class does not support disablePerEntryInvalidationEvents and has no eviction policy comparator support.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
@BinaryInterface
public class LegacyCacheConfig<K, V> implements TypedDataSerializable {

    private CacheConfig<K, V> config;

    @SuppressWarnings("unused")
    public LegacyCacheConfig() {
        config = new CacheConfig<K, V>();
    }

    public LegacyCacheConfig(CacheConfig<K, V> config) {
        this.config = config;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(config.getName());
        out.writeUTF(config.getManagerPrefix());
        out.writeUTF(config.getUriString());
        out.writeInt(config.getBackupCount());
        out.writeInt(config.getAsyncBackupCount());

        out.writeUTF(config.getInMemoryFormat().name());
        out.writeObject(new LegacyCacheEvictionConfig(config.getEvictionConfig()));

        out.writeObject(config.getWanReplicationRef());

        // SUPER
        out.writeObject(config.getKeyType());
        out.writeObject(config.getValueType());
        out.writeObject(config.getCacheLoaderFactory());
        out.writeObject(config.getCacheWriterFactory());
        out.writeObject(config.getExpiryPolicyFactory());

        out.writeBoolean(config.isReadThrough());
        out.writeBoolean(config.isWriteThrough());
        out.writeBoolean(config.isStoreByValue());
        out.writeBoolean(config.isManagementEnabled());
        out.writeBoolean(config.isStatisticsEnabled());
        out.writeBoolean(config.getHotRestartConfig().isEnabled());
        out.writeBoolean(config.getHotRestartConfig().isFsync());

        out.writeUTF(config.getQuorumName());

        Set<CacheEntryListenerConfiguration<K, V>> cacheEntryListenerConfigurations =
                (Set<CacheEntryListenerConfiguration<K, V>>) config.getCacheEntryListenerConfigurations();
        boolean listNotEmpty = cacheEntryListenerConfigurations != null && !cacheEntryListenerConfigurations.isEmpty();
        out.writeBoolean(listNotEmpty);
        if (listNotEmpty) {
            out.writeInt(cacheEntryListenerConfigurations.size());
            for (CacheEntryListenerConfiguration<K, V> cc : cacheEntryListenerConfigurations) {
                out.writeObject(cc);
            }
        }

        out.writeUTF(config.getMergePolicy());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readData(ObjectDataInput in) throws IOException {
        config.setName(in.readUTF());
        config.setManagerPrefix(in.readUTF());
        config.setUriString(in.readUTF());
        config.setBackupCount(in.readInt());
        config.setAsyncBackupCount(in.readInt());

        String resultInMemoryFormat = in.readUTF();
        config.setInMemoryFormat(InMemoryFormat.valueOf(resultInMemoryFormat));
        LegacyCacheEvictionConfig legacyConfig = in.readObject(LegacyCacheEvictionConfig.class);
        config.setEvictionConfig(legacyConfig.getConfig());

        config.setWanReplicationRef((WanReplicationRef) in.readObject());

        // SUPER
        config.setKeyType((Class<K>) in.readObject());
        config.setValueType((Class<V>) in.readObject());
        config.setCacheLoaderFactory((Factory<? extends CacheLoader<K, V>>) in.readObject());
        config.setCacheWriterFactory((Factory<? extends CacheWriter<? super K, ? super V>>) in.readObject());
        config.setExpiryPolicyFactory((Factory<? extends ExpiryPolicy>) in.readObject());

        config.setReadThrough(in.readBoolean());
        config.setWriteThrough(in.readBoolean());
        config.setStoreByValue(in.readBoolean());
        config.setManagementEnabled(in.readBoolean());
        config.setStatisticsEnabled(in.readBoolean());
        config.getHotRestartConfig().setEnabled(in.readBoolean());
        config.getHotRestartConfig().setFsync(in.readBoolean());

        config.setQuorumName(in.readUTF());

        final boolean listNotEmpty = in.readBoolean();
        if (listNotEmpty) {
            final int size = in.readInt();
            config.setListenerConfigurations();
            Set<CacheEntryListenerConfiguration<K, V>> listenerConfigurations =
                    (Set<CacheEntryListenerConfiguration<K, V>>) config.getCacheEntryListenerConfigurations();
            for (int i = 0; i < size; i++) {
                listenerConfigurations.add((CacheEntryListenerConfiguration<K, V>) in.readObject());
            }
        }

        config.setMergePolicy(in.readUTF());
    }

    @Override
    public Class getClassType() {
        return CacheConfig.class;
    }

    public CacheConfig<K, V> getConfigAndReset() {
        CacheConfig<K, V> actualConfig = config;
        this.config = null;
        return actualConfig;
    }
}
