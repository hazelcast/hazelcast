/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.cache.impl.CacheEventDataImpl;
import com.hazelcast.client.impl.protocol.codec.holder.ClusterStateHolder;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.CacheSimpleEntryListenerConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.querycache.event.DefaultQueryCacheEventData;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import static com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig.ExpiryPolicyType;
import static com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;

public final class CustomTypeFactory {

    private CustomTypeFactory() {
    }

    public static Address createAddress(String host, int port) {
        try {
            return new Address(host, port);
        } catch (UnknownHostException e) {
            throw new HazelcastException(e);
        }
    }

    public static CacheEventDataImpl createCacheEventData(String name, int cacheEventType, Data dataKey,
                                                          Data dataValue, Data dataOldValue, boolean oldValueAvailable) {
        return new CacheEventDataImpl(name, CacheEventType.getByType(cacheEventType), dataKey, dataValue,
                dataOldValue, oldValueAvailable);
    }

    public static TimedExpiryPolicyFactoryConfig createTimedExpiryPolicyFactoryConfig(String expiryPolicyType,
                                                                                        DurationConfig durationConfig) {
        return new TimedExpiryPolicyFactoryConfig(ExpiryPolicyType.valueOf(expiryPolicyType), durationConfig);
    }

    public static CacheSimpleEntryListenerConfig createCacheSimpleEntryListenerConfig(boolean oldValueRequired,
                                                                                      boolean synchronous,
                                                                                      String cacheEntryListenerFactory,
                                                                                      String cacheEntryEventFilterFactory) {
        CacheSimpleEntryListenerConfig config = new CacheSimpleEntryListenerConfig();
        config.setOldValueRequired(oldValueRequired);
        config.setSynchronous(synchronous);
        config.setCacheEntryListenerFactory(cacheEntryListenerFactory);
        config.setCacheEntryEventFilterFactory(cacheEntryEventFilterFactory);
        return config;
    }

    public static EventJournalConfig createEventJournalConfig(boolean enabled, int capacity, int timeToLiveSeconds) {
        EventJournalConfig config = new EventJournalConfig();
        config.setEnabled(enabled);
        config.setCapacity(capacity);
        config.setTimeToLiveSeconds(timeToLiveSeconds);
        return config;
    }

    public static HotRestartConfig createHotRestartConfig(boolean enabled, boolean fsync) {
        HotRestartConfig config = new HotRestartConfig();
        config.setEnabled(enabled);
        config.setFsync(fsync);
        return config;
    }

    public static MerkleTreeConfig createMerkleTreeConfig(boolean enabled, int depth) {
        MerkleTreeConfig config = new MerkleTreeConfig();
        config.setEnabled(enabled);
        config.setDepth(depth);
        return config;
    }

    public static NearCachePreloaderConfig createNearCachePreloaderConfig(boolean enabled, String directory,
                                                                          int storeInitialDelaySeconds,
                                                                          int storeIntervalSeconds) {
        NearCachePreloaderConfig config = new NearCachePreloaderConfig();
        config.setEnabled(enabled);
        config.setDirectory(directory);
        config.setStoreInitialDelaySeconds(storeInitialDelaySeconds);
        config.setStoreIntervalSeconds(storeIntervalSeconds);
        return config;
    }

    public static SimpleEntryView<Data, Data> createSimpleEntryView(Data key, Data value, long cost, long creationTime,
                                                        long expirationTime, long hits, long lastAccessTime,
                                                        long lastStoredTime, long lastUpdateTime, long version,
                                                        long ttl, long maxIdle) {
        SimpleEntryView<Data, Data> entryView = new SimpleEntryView<>();
        entryView.setKey(key);
        entryView.setValue(value);
        entryView.setCost(cost);
        entryView.setCreationTime(creationTime);
        entryView.setExpirationTime(expirationTime);
        entryView.setHits(hits);
        entryView.setLastAccessTime(lastAccessTime);
        entryView.setLastStoredTime(lastStoredTime);
        entryView.setLastUpdateTime(lastUpdateTime);
        entryView.setVersion(version);
        entryView.setTtl(ttl);
        entryView.setMaxIdle(maxIdle);
        return entryView;
    }

    public static DefaultQueryCacheEventData createQueryCacheEventData(Data dataKey, Data dataNewValue, long sequence,
                                                                int eventType, int partitionId) {
        DefaultQueryCacheEventData eventData = new DefaultQueryCacheEventData();
        eventData.setDataKey(dataKey);
        eventData.setDataNewValue(dataNewValue);
        eventData.setSequence(sequence);
        eventData.setEventType(eventType);
        eventData.setPartitionId(partitionId);
        return eventData;
    }

    public static DurationConfig createDurationConfig(long durationAmount, String timeUnit) {
        return new DurationConfig(durationAmount, TimeUnit.valueOf(timeUnit));
    }


    public static ClusterStateHolder createClusterStateHolder(String state) {
        return new ClusterStateHolder(ClusterState.valueOf(state));
    }
}
