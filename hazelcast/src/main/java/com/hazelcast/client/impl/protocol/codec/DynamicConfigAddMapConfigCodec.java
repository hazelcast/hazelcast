/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;
import com.hazelcast.client.impl.protocol.codec.custom.*;

import javax.annotation.Nullable;

import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

/*
 * This file is auto-generated by the Hazelcast Client Protocol Code Generator.
 * To change this file, edit the templates or the protocol
 * definitions on the https://github.com/hazelcast/hazelcast-client-protocol
 * and regenerate it.
 */

/**
 * Adds a new map configuration to a running cluster.
 * If a map configuration with the given {@code name} already exists, then
 * the new configuration is ignored and the existing one is preserved.
 */
@SuppressWarnings("unused")
@Generated("c57046e5ef64d919da836b49cd4b746c")
public final class DynamicConfigAddMapConfigCodec {
    //hex: 0x1B0C00
    public static final int REQUEST_MESSAGE_TYPE = 1772544;
    //hex: 0x1B0C01
    public static final int RESPONSE_MESSAGE_TYPE = 1772545;
    private static final int REQUEST_BACKUP_COUNT_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_ASYNC_BACKUP_COUNT_FIELD_OFFSET = REQUEST_BACKUP_COUNT_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_TIME_TO_LIVE_SECONDS_FIELD_OFFSET = REQUEST_ASYNC_BACKUP_COUNT_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_MAX_IDLE_SECONDS_FIELD_OFFSET = REQUEST_TIME_TO_LIVE_SECONDS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_READ_BACKUP_DATA_FIELD_OFFSET = REQUEST_MAX_IDLE_SECONDS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_MERGE_BATCH_SIZE_FIELD_OFFSET = REQUEST_READ_BACKUP_DATA_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int REQUEST_STATISTICS_ENABLED_FIELD_OFFSET = REQUEST_MERGE_BATCH_SIZE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_METADATA_POLICY_FIELD_OFFSET = REQUEST_STATISTICS_ENABLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int REQUEST_PER_ENTRY_STATS_ENABLED_FIELD_OFFSET = REQUEST_METADATA_POLICY_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_PER_ENTRY_STATS_ENABLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private DynamicConfigAddMapConfigCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * name of the map
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
         * map eviction configuration
         */
        public @Nullable com.hazelcast.client.impl.protocol.task.dynamicconfig.EvictionConfigHolder evictionConfig;

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
         * Name of a class implementing SplitBrainMergePolicy that handles merging of values for this cache
         * while recovering from network partitioning.
         */
        public java.lang.String mergePolicy;

        /**
         * Number of entries to be sent in a merge operation
         */
        public int mergeBatchSize;

        /**
         * data type used to store entries. Valid values are {@code BINARY},
         * {@code OBJECT} and {@code NATIVE}.
         */
        public java.lang.String inMemoryFormat;

        /**
         * entry listener configurations
         */
        public @Nullable java.util.List<com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder> listenerConfigs;

        /**
         * partition lost listener configurations
         */
        public @Nullable java.util.List<com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder> partitionLostListenerConfigs;

        /**
         * {@code true} to enable gathering of statistics, otherwise {@code false}
         */
        public boolean statisticsEnabled;

        /**
         * name of an existing configured split brain protection to be used to determine the minimum
         * number of members required in the cluster for the map to remain functional.
         * When {@code null}, split brain protection does not apply to this map's operations.
         */
        public @Nullable java.lang.String splitBrainProtectionName;

        /**
         * configuration of backing map store or {@code null} for none
         */
        public @Nullable com.hazelcast.client.impl.protocol.task.dynamicconfig.MapStoreConfigHolder mapStoreConfig;

        /**
         * configuration of near cache or {@code null} for none
         */
        public @Nullable com.hazelcast.client.impl.protocol.task.dynamicconfig.NearCacheConfigHolder nearCacheConfig;

        /**
         * reference to an existing WAN replication configuration
         */
        public @Nullable com.hazelcast.config.WanReplicationRef wanReplicationRef;

        /**
         * index configurations
         */
        public @Nullable java.util.List<com.hazelcast.config.IndexConfig> indexConfigs;

        /**
         * map attributes
         */
        public @Nullable java.util.List<com.hazelcast.config.AttributeConfig> attributeConfigs;

        /**
         * configurations for query caches on this map
         */
        public @Nullable java.util.List<com.hazelcast.client.impl.protocol.task.dynamicconfig.QueryCacheConfigHolder> queryCacheConfigs;

        /**
         * name of class implementing {@code com.hazelcast.core.PartitioningStrategy}
         * or {@code null}
         */
        public @Nullable java.lang.String partitioningStrategyClassName;

        /**
         * a serialized instance of a partitioning strategy
         */
        public @Nullable com.hazelcast.internal.serialization.Data partitioningStrategyImplementation;

        /**
         * hot restart configuration
         */
        public @Nullable com.hazelcast.config.HotRestartConfig hotRestartConfig;

        /**
         * event journal configuration
         */
        public @Nullable com.hazelcast.config.EventJournalConfig eventJournalConfig;

        /**
         * merkle tree configuration
         */
        public @Nullable com.hazelcast.config.MerkleTreeConfig merkleTreeConfig;

        /**
         * metadata policy configuration for the supported data types. Valid values
         * are {@code CREATE_ON_UPDATE} and {@code OFF}
         */
        public int metadataPolicy;

        /**
         * {@code true} to enable entry level statistics for the entries of this map.
         *  otherwise {@code false}. Default value is {@code false}
         */
        public boolean perEntryStatsEnabled;

        /**
         * Data persistence configuration
         */
        public com.hazelcast.config.DataPersistenceConfig dataPersistenceConfig;

        /**
         * Tiered-Store configuration
         */
        public com.hazelcast.config.TieredStoreConfig tieredStoreConfig;

        /**
         * List of attributes used for creating AttributePartitioningStrategy.
         */
        public @Nullable java.util.List<com.hazelcast.config.PartitioningAttributeConfig> partitioningAttributeConfigs;

        /**
         * Name of the namespace applied to this instance.
         */
        public @Nullable java.lang.String namespace;

        /**
         * True if the perEntryStatsEnabled is received from the client, false otherwise.
         * If this is false, perEntryStatsEnabled has the default value for its type.
         */
        public boolean isPerEntryStatsEnabledExists;

        /**
         * True if the dataPersistenceConfig is received from the client, false otherwise.
         * If this is false, dataPersistenceConfig has the default value for its type.
         */
        public boolean isDataPersistenceConfigExists;

        /**
         * True if the tieredStoreConfig is received from the client, false otherwise.
         * If this is false, tieredStoreConfig has the default value for its type.
         */
        public boolean isTieredStoreConfigExists;

        /**
         * True if the partitioningAttributeConfigs is received from the client, false otherwise.
         * If this is false, partitioningAttributeConfigs has the default value for its type.
         */
        public boolean isPartitioningAttributeConfigsExists;

        /**
         * True if the namespace is received from the client, false otherwise.
         * If this is false, namespace has the default value for its type.
         */
        public boolean isNamespaceExists;
    }

    public static ClientMessage encodeRequest(java.lang.String name, int backupCount, int asyncBackupCount, int timeToLiveSeconds, int maxIdleSeconds, @Nullable com.hazelcast.client.impl.protocol.task.dynamicconfig.EvictionConfigHolder evictionConfig, boolean readBackupData, java.lang.String cacheDeserializedValues, java.lang.String mergePolicy, int mergeBatchSize, java.lang.String inMemoryFormat, @Nullable java.util.Collection<com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder> listenerConfigs, @Nullable java.util.Collection<com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder> partitionLostListenerConfigs, boolean statisticsEnabled, @Nullable java.lang.String splitBrainProtectionName, @Nullable com.hazelcast.client.impl.protocol.task.dynamicconfig.MapStoreConfigHolder mapStoreConfig, @Nullable com.hazelcast.client.impl.protocol.task.dynamicconfig.NearCacheConfigHolder nearCacheConfig, @Nullable com.hazelcast.config.WanReplicationRef wanReplicationRef, @Nullable java.util.Collection<com.hazelcast.config.IndexConfig> indexConfigs, @Nullable java.util.Collection<com.hazelcast.config.AttributeConfig> attributeConfigs, @Nullable java.util.Collection<com.hazelcast.client.impl.protocol.task.dynamicconfig.QueryCacheConfigHolder> queryCacheConfigs, @Nullable java.lang.String partitioningStrategyClassName, @Nullable com.hazelcast.internal.serialization.Data partitioningStrategyImplementation, @Nullable com.hazelcast.config.HotRestartConfig hotRestartConfig, @Nullable com.hazelcast.config.EventJournalConfig eventJournalConfig, @Nullable com.hazelcast.config.MerkleTreeConfig merkleTreeConfig, int metadataPolicy, boolean perEntryStatsEnabled, com.hazelcast.config.DataPersistenceConfig dataPersistenceConfig, com.hazelcast.config.TieredStoreConfig tieredStoreConfig, @Nullable java.util.Collection<com.hazelcast.config.PartitioningAttributeConfig> partitioningAttributeConfigs, @Nullable java.lang.String namespace) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setContainsSerializedDataInRequest(true);
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("DynamicConfig.AddMapConfig");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeInt(initialFrame.content, REQUEST_BACKUP_COUNT_FIELD_OFFSET, backupCount);
        encodeInt(initialFrame.content, REQUEST_ASYNC_BACKUP_COUNT_FIELD_OFFSET, asyncBackupCount);
        encodeInt(initialFrame.content, REQUEST_TIME_TO_LIVE_SECONDS_FIELD_OFFSET, timeToLiveSeconds);
        encodeInt(initialFrame.content, REQUEST_MAX_IDLE_SECONDS_FIELD_OFFSET, maxIdleSeconds);
        encodeBoolean(initialFrame.content, REQUEST_READ_BACKUP_DATA_FIELD_OFFSET, readBackupData);
        encodeInt(initialFrame.content, REQUEST_MERGE_BATCH_SIZE_FIELD_OFFSET, mergeBatchSize);
        encodeBoolean(initialFrame.content, REQUEST_STATISTICS_ENABLED_FIELD_OFFSET, statisticsEnabled);
        encodeInt(initialFrame.content, REQUEST_METADATA_POLICY_FIELD_OFFSET, metadataPolicy);
        encodeBoolean(initialFrame.content, REQUEST_PER_ENTRY_STATS_ENABLED_FIELD_OFFSET, perEntryStatsEnabled);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        CodecUtil.encodeNullable(clientMessage, evictionConfig, EvictionConfigHolderCodec::encode);
        StringCodec.encode(clientMessage, cacheDeserializedValues);
        StringCodec.encode(clientMessage, mergePolicy);
        StringCodec.encode(clientMessage, inMemoryFormat);
        ListMultiFrameCodec.encodeNullable(clientMessage, listenerConfigs, ListenerConfigHolderCodec::encode);
        ListMultiFrameCodec.encodeNullable(clientMessage, partitionLostListenerConfigs, ListenerConfigHolderCodec::encode);
        CodecUtil.encodeNullable(clientMessage, splitBrainProtectionName, StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, mapStoreConfig, MapStoreConfigHolderCodec::encode);
        CodecUtil.encodeNullable(clientMessage, nearCacheConfig, NearCacheConfigHolderCodec::encode);
        CodecUtil.encodeNullable(clientMessage, wanReplicationRef, WanReplicationRefCodec::encode);
        ListMultiFrameCodec.encodeNullable(clientMessage, indexConfigs, IndexConfigCodec::encode);
        ListMultiFrameCodec.encodeNullable(clientMessage, attributeConfigs, AttributeConfigCodec::encode);
        ListMultiFrameCodec.encodeNullable(clientMessage, queryCacheConfigs, QueryCacheConfigHolderCodec::encode);
        CodecUtil.encodeNullable(clientMessage, partitioningStrategyClassName, StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, partitioningStrategyImplementation, DataCodec::encode);
        CodecUtil.encodeNullable(clientMessage, hotRestartConfig, HotRestartConfigCodec::encode);
        CodecUtil.encodeNullable(clientMessage, eventJournalConfig, EventJournalConfigCodec::encode);
        CodecUtil.encodeNullable(clientMessage, merkleTreeConfig, MerkleTreeConfigCodec::encode);
        DataPersistenceConfigCodec.encode(clientMessage, dataPersistenceConfig);
        TieredStoreConfigCodec.encode(clientMessage, tieredStoreConfig);
        ListMultiFrameCodec.encodeNullable(clientMessage, partitioningAttributeConfigs, PartitioningAttributeConfigCodec::encode);
        CodecUtil.encodeNullable(clientMessage, namespace, StringCodec::encode);
        return clientMessage;
    }

    public static DynamicConfigAddMapConfigCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.backupCount = decodeInt(initialFrame.content, REQUEST_BACKUP_COUNT_FIELD_OFFSET);
        request.asyncBackupCount = decodeInt(initialFrame.content, REQUEST_ASYNC_BACKUP_COUNT_FIELD_OFFSET);
        request.timeToLiveSeconds = decodeInt(initialFrame.content, REQUEST_TIME_TO_LIVE_SECONDS_FIELD_OFFSET);
        request.maxIdleSeconds = decodeInt(initialFrame.content, REQUEST_MAX_IDLE_SECONDS_FIELD_OFFSET);
        request.readBackupData = decodeBoolean(initialFrame.content, REQUEST_READ_BACKUP_DATA_FIELD_OFFSET);
        request.mergeBatchSize = decodeInt(initialFrame.content, REQUEST_MERGE_BATCH_SIZE_FIELD_OFFSET);
        request.statisticsEnabled = decodeBoolean(initialFrame.content, REQUEST_STATISTICS_ENABLED_FIELD_OFFSET);
        request.metadataPolicy = decodeInt(initialFrame.content, REQUEST_METADATA_POLICY_FIELD_OFFSET);
        if (initialFrame.content.length >= REQUEST_PER_ENTRY_STATS_ENABLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES) {
            request.perEntryStatsEnabled = decodeBoolean(initialFrame.content, REQUEST_PER_ENTRY_STATS_ENABLED_FIELD_OFFSET);
            request.isPerEntryStatsEnabledExists = true;
        } else {
            request.isPerEntryStatsEnabledExists = false;
        }
        request.name = StringCodec.decode(iterator);
        request.evictionConfig = CodecUtil.decodeNullable(iterator, EvictionConfigHolderCodec::decode);
        request.cacheDeserializedValues = StringCodec.decode(iterator);
        request.mergePolicy = StringCodec.decode(iterator);
        request.inMemoryFormat = StringCodec.decode(iterator);
        request.listenerConfigs = ListMultiFrameCodec.decodeNullable(iterator, ListenerConfigHolderCodec::decode);
        request.partitionLostListenerConfigs = ListMultiFrameCodec.decodeNullable(iterator, ListenerConfigHolderCodec::decode);
        request.splitBrainProtectionName = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        request.mapStoreConfig = CodecUtil.decodeNullable(iterator, MapStoreConfigHolderCodec::decode);
        request.nearCacheConfig = CodecUtil.decodeNullable(iterator, NearCacheConfigHolderCodec::decode);
        request.wanReplicationRef = CodecUtil.decodeNullable(iterator, WanReplicationRefCodec::decode);
        request.indexConfigs = ListMultiFrameCodec.decodeNullable(iterator, IndexConfigCodec::decode);
        request.attributeConfigs = ListMultiFrameCodec.decodeNullable(iterator, AttributeConfigCodec::decode);
        request.queryCacheConfigs = ListMultiFrameCodec.decodeNullable(iterator, QueryCacheConfigHolderCodec::decode);
        request.partitioningStrategyClassName = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        request.partitioningStrategyImplementation = CodecUtil.decodeNullable(iterator, DataCodec::decode);
        request.hotRestartConfig = CodecUtil.decodeNullable(iterator, HotRestartConfigCodec::decode);
        request.eventJournalConfig = CodecUtil.decodeNullable(iterator, EventJournalConfigCodec::decode);
        request.merkleTreeConfig = CodecUtil.decodeNullable(iterator, MerkleTreeConfigCodec::decode);
        if (iterator.hasNext()) {
            request.dataPersistenceConfig = DataPersistenceConfigCodec.decode(iterator);
            request.isDataPersistenceConfigExists = true;
        } else {
            request.isDataPersistenceConfigExists = false;
        }
        if (iterator.hasNext()) {
            request.tieredStoreConfig = TieredStoreConfigCodec.decode(iterator);
            request.isTieredStoreConfigExists = true;
        } else {
            request.isTieredStoreConfigExists = false;
        }
        if (iterator.hasNext()) {
            request.partitioningAttributeConfigs = ListMultiFrameCodec.decodeNullable(iterator, PartitioningAttributeConfigCodec::decode);
            request.isPartitioningAttributeConfigsExists = true;
        } else {
            request.isPartitioningAttributeConfigsExists = false;
        }
        if (iterator.hasNext()) {
            request.namespace = CodecUtil.decodeNullable(iterator, StringCodec::decode);
            request.isNamespaceExists = true;
        } else {
            request.isNamespaceExists = false;
        }
        return request;
    }

    public static ClientMessage encodeResponse() {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        return clientMessage;
    }
}
