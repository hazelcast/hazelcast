/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cluster.Address;
import com.hazelcast.config.BitmapIndexOptions;
import com.hazelcast.config.BitmapIndexOptions.UniqueKeyTransformation;
import com.hazelcast.config.CacheSimpleEntryListenerConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.management.dto.ClientBwListEntryDTO;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.compact.FieldDescriptor;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.querycache.event.DefaultQueryCacheEventData;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import static com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import static com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig.ExpiryPolicyType;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public final class CustomTypeFactory {

    private CustomTypeFactory() {
    }

    public static Address createAddress(String host, int port) {
        return Address.createUnresolvedAddress(host, port);
    }

    public static CacheEventDataImpl createCacheEventData(String name, int cacheEventType, Data dataKey,
                                                          Data dataValue, Data dataOldValue, boolean oldValueAvailable) {
        return new CacheEventDataImpl(name, CacheEventType.getByType(cacheEventType), dataKey, dataValue,
                dataOldValue, oldValueAvailable);
    }

    public static TimedExpiryPolicyFactoryConfig createTimedExpiryPolicyFactoryConfig(int expiryPolicyType,
                                                                                      DurationConfig durationConfig) {
        return new TimedExpiryPolicyFactoryConfig(ExpiryPolicyType.getById(expiryPolicyType), durationConfig);
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

    public static MerkleTreeConfig createMerkleTreeConfig(boolean enabled, int depth,
                                                          boolean isEnabledSetExists, boolean isEnabledSet) {
        MerkleTreeConfig config = new MerkleTreeConfig();
        if (!isEnabledSetExists || isEnabledSet) {
            config.setEnabled(enabled);
        }
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

    public static DurationConfig createDurationConfig(long durationAmount, int timeUnitId) {
        TimeUnit timeUnit;
        if (timeUnitId == 0) {
            timeUnit = TimeUnit.NANOSECONDS;
        } else if (timeUnitId == 1) {
            timeUnit = TimeUnit.MICROSECONDS;
        } else if (timeUnitId == 2) {
            timeUnit = TimeUnit.MILLISECONDS;
        } else if (timeUnitId == 3) {
            timeUnit = TimeUnit.SECONDS;
        } else if (timeUnitId == 4) {
            timeUnit = TimeUnit.MINUTES;
        } else if (timeUnitId == 5) {
            timeUnit = TimeUnit.HOURS;
        } else if (timeUnitId == 6) {
            timeUnit = TimeUnit.DAYS;
        } else {
            timeUnit = null;
        }
        return new DurationConfig(durationAmount, timeUnit);
    }

    public static IndexConfig createIndexConfig(String name, int type, List<String> attributes,
                                                BitmapIndexOptions bitmapIndexOptions) {
        IndexType type0 = IndexType.getById(type);

        return new IndexConfig()
                .setName(name)
                .setType(type0)
                .setAttributes(attributes)
                .setBitmapIndexOptions(bitmapIndexOptions);
    }

    public static BitmapIndexOptions createBitmapIndexOptions(String uniqueKey, int uniqueKeyTransformation) {
        UniqueKeyTransformation resolvedUniqueKeyTransformation = UniqueKeyTransformation.fromId(uniqueKeyTransformation);
        return new BitmapIndexOptions().setUniqueKey(uniqueKey).setUniqueKeyTransformation(resolvedUniqueKeyTransformation);
    }

    public static ClientBwListEntryDTO createClientBwListEntry(int type, String value) {
        ClientBwListEntryDTO.Type entryType = ClientBwListEntryDTO.Type.getById(type);
        if (entryType == null) {
            throw new HazelcastException("Unexpected client B/W list entry type = [" + type + "]");
        }
        return new ClientBwListEntryDTO(entryType, value);
    }

    public static EndpointQualifier createEndpointQualifier(int type, String identifier) {
        ProtocolType protocolType = ProtocolType.getById(type);
        if (protocolType == null) {
            throw new HazelcastException("Unexpected protocol type = [" + type + "]");
        }
        return EndpointQualifier.resolve(protocolType, identifier);
    }

    public static SqlColumnMetadata createSqlColumnMetadata(String name, int type, boolean isNullableExists, boolean nullability) {
        SqlColumnType sqlColumnType = SqlColumnType.getById(type);

        if (sqlColumnType == null) {
            throw new HazelcastException("Unexpected SQL column type = [" + type + "]");
        }

        if (isNullableExists) {
            return new SqlColumnMetadata(name, sqlColumnType, nullability);
        }

        return new SqlColumnMetadata(name, sqlColumnType, true);
    }

    public static FieldDescriptor createFieldDescriptor(@Nonnull String fieldName, int id) {
        FieldKind fieldKind = FieldKind.get(id);
        return new FieldDescriptor(fieldName, fieldKind);
    }

    public static Schema createSchema(String typeName, List<FieldDescriptor> fields) {
        TreeMap<String, FieldDescriptor> map = new TreeMap<>(Comparator.naturalOrder());
        for (FieldDescriptor field : fields) {
            map.put(field.getFieldName(), field);
        }
        return new Schema(typeName, map);
    }

    public static HazelcastJsonValue createHazelcastJsonValue(String value) {
        return new HazelcastJsonValue(value);
    }
}
