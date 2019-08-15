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

package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

/**
 * Adds a new map configuration to a running cluster.
 * If a map configuration with the given {@code name} already exists, then
 * the new configuration is ignored and the existing one is preserved.
 */
public final class DynamicConfigAddMapConfigCodec {
    //hex: 0x1E0E00
    public static final int REQUEST_MESSAGE_TYPE = 1969664;
    //hex: 0x1E0E01
    public static final int RESPONSE_MESSAGE_TYPE = 1969665;
    private static final int REQUEST_BACKUP_COUNT_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_ASYNC_BACKUP_COUNT_FIELD_OFFSET = REQUEST_BACKUP_COUNT_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_TIME_TO_LIVE_SECONDS_FIELD_OFFSET = REQUEST_ASYNC_BACKUP_COUNT_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_MAX_IDLE_SECONDS_FIELD_OFFSET = REQUEST_TIME_TO_LIVE_SECONDS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_READ_BACKUP_DATA_FIELD_OFFSET = REQUEST_MAX_IDLE_SECONDS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_STATISTICS_ENABLED_FIELD_OFFSET = REQUEST_READ_BACKUP_DATA_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int REQUEST_MAX_SIZE_CONFIG_SIZE_FIELD_OFFSET = REQUEST_STATISTICS_ENABLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int REQUEST_MERGE_BATCH_SIZE_FIELD_OFFSET = REQUEST_MAX_SIZE_CONFIG_SIZE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_METADATA_POLICY_FIELD_OFFSET = REQUEST_MERGE_BATCH_SIZE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_METADATA_POLICY_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private DynamicConfigAddMapConfigCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * TODO DOC
         */
        public java.lang.String name;

        /**
         * number of synchronous backups
         */
        public int backupCount;

        /**
         * number of asynchronous backups
         */
        public int asyncBackupCount;

        /**
         * maximum number of seconds for each entry to stay in the map.
         */
        public int timeToLiveSeconds;

        /**
         * maximum number of seconds for each entry to stay idle in the map
         */
        public int maxIdleSeconds;

        /**
         * eviction policy. Valid values: {@code NONE} (no eviction), {@code LRU}
         * (Least Recently Used), {@code LFU} (Least Frequently Used),
         * {@code RANDOM} (evict random entry).
         */
        public java.lang.String evictionPolicy;

        /**
         * {@code true} to enable reading local backup entries, {@code false} otherwise
         */
        public boolean readBackupData;

        /**
         * control caching of de-serialized values. Valid values are {@code NEVER}
         * (Never cache de-serialized object), {@code INDEX_ONLY} (Cache values only
         * when they are inserted into an index) and {@code ALWAYS} (Always cache
         * de-serialized values
         */
        public java.lang.String cacheDeserializedValues;

        /**
         * class name of a class implementing
         * {@code com.hazelcast.map.merge.MapMergePolicy} to merge entries
         * while recovering from a split brain
         */
        public java.lang.String mergePolicy;

        /**
         * data type used to store entries. Valid values are {@code BINARY},
         * {@code OBJECT} and {@code NATIVE}.
         */
        public java.lang.String inMemoryFormat;

        /**
         * entry listener configurations
         */
        public java.util.List<com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder> listenerConfigs;

        /**
         * partition lost listener configurations
         */
        public java.util.List<com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder> partitionLostListenerConfigs;

        /**
         * {@code true} to enable gathering of statistics, otherwise {@code false}
         */
        public boolean statisticsEnabled;

        /**
         * name of an existing configured quorum to be used to determine the minimum
         * number of members required in the cluster for the map to remain functional.
         * When {@code null}, quorum does not apply to this map's operations.
         */
        public java.lang.String quorumName;

        /**
         * custom {@code com.hazelcast.map.eviction.MapEvictionPolicy} implementation
         * or {@code null}
         */
        public com.hazelcast.nio.serialization.Data mapEvictionPolicy;

        /**
         * maximum size policy. Valid values are {@code PER_NODE},
         * {@code PER_PARTITION}, {@code USED_HEAP_PERCENTAGE}, {@code USED_HEAP_SIZE},
         * {@code FREE_HEAP_PERCENTAGE}, {@code FREE_HEAP_SIZE},
         * {@code USED_NATIVE_MEMORY_SIZE}, {@code USED_NATIVE_MEMORY_PERCENTAGE},
         * {@code FREE_NATIVE_MEMORY_SIZE}, {@code FREE_NATIVE_MEMORY_PERCENTAGE}.
         */
        public java.lang.String maxSizeConfigMaxSizePolicy;

        /**
         * maximum size of map
         */
        public int maxSizeConfigSize;

        /**
         * configuration of backing map store or {@code null} for none
         */
        public com.hazelcast.client.impl.protocol.task.dynamicconfig.MapStoreConfigHolder mapStoreConfig;

        /**
         * configuration of near cache or {@code null} for none
         */
        public com.hazelcast.client.impl.protocol.task.dynamicconfig.NearCacheConfigHolder nearCacheConfig;

        /**
         * reference to an existing WAN replication configuration
         */
        public com.hazelcast.config.WanReplicationRef wanReplicationRef;

        /**
         * map index configurations
         */
        public java.util.List<com.hazelcast.config.MapIndexConfig> mapIndexConfigs;

        /**
         * map attributes
         */
        public java.util.List<com.hazelcast.config.MapAttributeConfig> mapAttributeConfigs;

        /**
         * configurations for query caches on this map
         */
        public java.util.List<com.hazelcast.client.impl.protocol.task.dynamicconfig.QueryCacheConfigHolder> queryCacheConfigs;

        /**
         * name of class implementing {@code com.hazelcast.core.PartitioningStrategy}
         * or {@code null}
         */
        public java.lang.String partitioningStrategyClassName;

        /**
         * a serialized instance of a partitioning strategy
         */
        public com.hazelcast.nio.serialization.Data partitioningStrategyImplementation;

        /**
         * hot restart configuration
         */
        public com.hazelcast.config.HotRestartConfig hotRestartConfig;

        /**
         * Event Journal configuration
         */
        public com.hazelcast.config.EventJournalConfig eventJournalConfig;

        /**
         * - merkle tree configuration
         */
        public com.hazelcast.config.MerkleTreeConfig merkleTreeConfig;

        /**
         * TODO DOC
         */
        public int mergeBatchSize;

        /**
         * metadata policy configuration for the supported data types. Valid values
         * are {@code CREATE_ON_UPDATE} and {@code OFF}
         */
        public int metadataPolicy;
    }

    public static ClientMessage encodeRequest(java.lang.String name, int backupCount, int asyncBackupCount, int timeToLiveSeconds, int maxIdleSeconds, java.lang.String evictionPolicy, boolean readBackupData, java.lang.String cacheDeserializedValues, java.lang.String mergePolicy, java.lang.String inMemoryFormat, java.util.Collection<com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder> listenerConfigs, java.util.Collection<com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder> partitionLostListenerConfigs, boolean statisticsEnabled, java.lang.String quorumName, com.hazelcast.nio.serialization.Data mapEvictionPolicy, java.lang.String maxSizeConfigMaxSizePolicy, int maxSizeConfigSize, com.hazelcast.client.impl.protocol.task.dynamicconfig.MapStoreConfigHolder mapStoreConfig, com.hazelcast.client.impl.protocol.task.dynamicconfig.NearCacheConfigHolder nearCacheConfig, com.hazelcast.config.WanReplicationRef wanReplicationRef, java.util.Collection<com.hazelcast.config.MapIndexConfig> mapIndexConfigs, java.util.Collection<com.hazelcast.config.MapAttributeConfig> mapAttributeConfigs, java.util.Collection<com.hazelcast.client.impl.protocol.task.dynamicconfig.QueryCacheConfigHolder> queryCacheConfigs, java.lang.String partitioningStrategyClassName, com.hazelcast.nio.serialization.Data partitioningStrategyImplementation, com.hazelcast.config.HotRestartConfig hotRestartConfig, com.hazelcast.config.EventJournalConfig eventJournalConfig, com.hazelcast.config.MerkleTreeConfig merkleTreeConfig, int mergeBatchSize, int metadataPolicy) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("DynamicConfig.AddMapConfig");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, REQUEST_BACKUP_COUNT_FIELD_OFFSET, backupCount);
        encodeInt(initialFrame.content, REQUEST_ASYNC_BACKUP_COUNT_FIELD_OFFSET, asyncBackupCount);
        encodeInt(initialFrame.content, REQUEST_TIME_TO_LIVE_SECONDS_FIELD_OFFSET, timeToLiveSeconds);
        encodeInt(initialFrame.content, REQUEST_MAX_IDLE_SECONDS_FIELD_OFFSET, maxIdleSeconds);
        encodeBoolean(initialFrame.content, REQUEST_READ_BACKUP_DATA_FIELD_OFFSET, readBackupData);
        encodeBoolean(initialFrame.content, REQUEST_STATISTICS_ENABLED_FIELD_OFFSET, statisticsEnabled);
        encodeInt(initialFrame.content, REQUEST_MAX_SIZE_CONFIG_SIZE_FIELD_OFFSET, maxSizeConfigSize);
        encodeInt(initialFrame.content, REQUEST_MERGE_BATCH_SIZE_FIELD_OFFSET, mergeBatchSize);
        encodeInt(initialFrame.content, REQUEST_METADATA_POLICY_FIELD_OFFSET, metadataPolicy);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        StringCodec.encode(clientMessage, evictionPolicy);
        StringCodec.encode(clientMessage, cacheDeserializedValues);
        StringCodec.encode(clientMessage, mergePolicy);
        StringCodec.encode(clientMessage, inMemoryFormat);
        ListMultiFrameCodec.encodeNullable(clientMessage, listenerConfigs, ListenerConfigHolderCodec::encode);
        ListMultiFrameCodec.encodeNullable(clientMessage, partitionLostListenerConfigs, ListenerConfigHolderCodec::encode);
        CodecUtil.encodeNullable(clientMessage, quorumName, StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, mapEvictionPolicy, DataCodec::encode);
        StringCodec.encode(clientMessage, maxSizeConfigMaxSizePolicy);
        CodecUtil.encodeNullable(clientMessage, mapStoreConfig, MapStoreConfigHolderCodec::encode);
        CodecUtil.encodeNullable(clientMessage, nearCacheConfig, NearCacheConfigHolderCodec::encode);
        CodecUtil.encodeNullable(clientMessage, wanReplicationRef, WanReplicationRefCodec::encode);
        ListMultiFrameCodec.encodeNullable(clientMessage, mapIndexConfigs, MapIndexConfigCodec::encode);
        ListMultiFrameCodec.encodeNullable(clientMessage, mapAttributeConfigs, MapAttributeConfigCodec::encode);
        ListMultiFrameCodec.encodeNullable(clientMessage, queryCacheConfigs, QueryCacheConfigHolderCodec::encode);
        CodecUtil.encodeNullable(clientMessage, partitioningStrategyClassName, StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, partitioningStrategyImplementation, DataCodec::encode);
        CodecUtil.encodeNullable(clientMessage, hotRestartConfig, HotRestartConfigCodec::encode);
        CodecUtil.encodeNullable(clientMessage, eventJournalConfig, EventJournalConfigCodec::encode);
        CodecUtil.encodeNullable(clientMessage, merkleTreeConfig, MerkleTreeConfigCodec::encode);
        return clientMessage;
    }

    public static DynamicConfigAddMapConfigCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.backupCount = decodeInt(initialFrame.content, REQUEST_BACKUP_COUNT_FIELD_OFFSET);
        request.asyncBackupCount = decodeInt(initialFrame.content, REQUEST_ASYNC_BACKUP_COUNT_FIELD_OFFSET);
        request.timeToLiveSeconds = decodeInt(initialFrame.content, REQUEST_TIME_TO_LIVE_SECONDS_FIELD_OFFSET);
        request.maxIdleSeconds = decodeInt(initialFrame.content, REQUEST_MAX_IDLE_SECONDS_FIELD_OFFSET);
        request.readBackupData = decodeBoolean(initialFrame.content, REQUEST_READ_BACKUP_DATA_FIELD_OFFSET);
        request.statisticsEnabled = decodeBoolean(initialFrame.content, REQUEST_STATISTICS_ENABLED_FIELD_OFFSET);
        request.maxSizeConfigSize = decodeInt(initialFrame.content, REQUEST_MAX_SIZE_CONFIG_SIZE_FIELD_OFFSET);
        request.mergeBatchSize = decodeInt(initialFrame.content, REQUEST_MERGE_BATCH_SIZE_FIELD_OFFSET);
        request.metadataPolicy = decodeInt(initialFrame.content, REQUEST_METADATA_POLICY_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        request.evictionPolicy = StringCodec.decode(iterator);
        request.cacheDeserializedValues = StringCodec.decode(iterator);
        request.mergePolicy = StringCodec.decode(iterator);
        request.inMemoryFormat = StringCodec.decode(iterator);
        request.listenerConfigs = ListMultiFrameCodec.decodeNullable(iterator, ListenerConfigHolderCodec::decode);
        request.partitionLostListenerConfigs = ListMultiFrameCodec.decodeNullable(iterator, ListenerConfigHolderCodec::decode);
        request.quorumName = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        request.mapEvictionPolicy = CodecUtil.decodeNullable(iterator, DataCodec::decode);
        request.maxSizeConfigMaxSizePolicy = StringCodec.decode(iterator);
        request.mapStoreConfig = CodecUtil.decodeNullable(iterator, MapStoreConfigHolderCodec::decode);
        request.nearCacheConfig = CodecUtil.decodeNullable(iterator, NearCacheConfigHolderCodec::decode);
        request.wanReplicationRef = CodecUtil.decodeNullable(iterator, WanReplicationRefCodec::decode);
        request.mapIndexConfigs = ListMultiFrameCodec.decodeNullable(iterator, MapIndexConfigCodec::decode);
        request.mapAttributeConfigs = ListMultiFrameCodec.decodeNullable(iterator, MapAttributeConfigCodec::decode);
        request.queryCacheConfigs = ListMultiFrameCodec.decodeNullable(iterator, QueryCacheConfigHolderCodec::decode);
        request.partitioningStrategyClassName = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        request.partitioningStrategyImplementation = CodecUtil.decodeNullable(iterator, DataCodec::decode);
        request.hotRestartConfig = CodecUtil.decodeNullable(iterator, HotRestartConfigCodec::decode);
        request.eventJournalConfig = CodecUtil.decodeNullable(iterator, EventJournalConfigCodec::decode);
        request.merkleTreeConfig = CodecUtil.decodeNullable(iterator, MerkleTreeConfigCodec::decode);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {
    }

    public static ClientMessage encodeResponse() {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        return clientMessage;
    }

    public static DynamicConfigAddMapConfigCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        ResponseParameters response = new ResponseParameters();
        //empty initial frame
        iterator.next();
        return response;
    }

}
