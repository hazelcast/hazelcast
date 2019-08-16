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
 * Adds a new cache configuration to a running cluster.
 * If a cache configuration with the given {@code name} already exists, then
 * the new configuration is ignored and the existing one is preserved.
 */
public final class DynamicConfigAddCacheConfigCodec {
    //hex: 0x1E1000
    public static final int REQUEST_MESSAGE_TYPE = 1970176;
    //hex: 0x1E1001
    public static final int RESPONSE_MESSAGE_TYPE = 1970177;
    private static final int REQUEST_STATISTICS_ENABLED_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_MANAGEMENT_ENABLED_FIELD_OFFSET = REQUEST_STATISTICS_ENABLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int REQUEST_READ_THROUGH_FIELD_OFFSET = REQUEST_MANAGEMENT_ENABLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int REQUEST_WRITE_THROUGH_FIELD_OFFSET = REQUEST_READ_THROUGH_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int REQUEST_BACKUP_COUNT_FIELD_OFFSET = REQUEST_WRITE_THROUGH_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int REQUEST_ASYNC_BACKUP_COUNT_FIELD_OFFSET = REQUEST_BACKUP_COUNT_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_DISABLE_PER_ENTRY_INVALIDATION_EVENTS_FIELD_OFFSET = REQUEST_ASYNC_BACKUP_COUNT_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_DISABLE_PER_ENTRY_INVALIDATION_EVENTS_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private DynamicConfigAddCacheConfigCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * cache name
         */
        public java.lang.String name;

        /**
         * class name of key type
         */
        public java.lang.String keyType;

        /**
         * class name of value type
         */
        public java.lang.String valueType;

        /**
         * {@code true} to enable gathering of statistics, otherwise {@code false}
         */
        public boolean statisticsEnabled;

        /**
         * {@code true} to enable management interface on this cache or {@code false}
         */
        public boolean managementEnabled;

        /**
         * {@code true} to enable read through from a {@code CacheLoader}
         */
        public boolean readThrough;

        /**
         * {@code true} to enable write through to a {@code CacheWriter}
         */
        public boolean writeThrough;

        /**
         * name of cache loader factory class, if one is configured
         */
        public java.lang.String cacheLoaderFactory;

        /**
         * name of cache writer factory class, if one is configured
         */
        public java.lang.String cacheWriterFactory;

        /**
         * Factory                    name of cache loader factory class, if one is configured
         */
        public java.lang.String cacheLoader;

        /**
         * Factory                    name of cache writer factory class, if one is configured
         */
        public java.lang.String cacheWriter;

        /**
         * number of synchronous backups
         */
        public int backupCount;

        /**
         * number of asynchronous backups
         */
        public int asyncBackupCount;

        /**
         * data type used to store entries. Valid values are {@code BINARY},
         * {@code OBJECT} and {@code NATIVE}.
         */
        public java.lang.String inMemoryFormat;

        /**
         * name of an existing configured quorum to be used to determine the minimum
         * number of members required in the cluster for the cache to remain functional.
         * When {@code null}, quorum does not apply to this cache's operations.
         */
        public java.lang.String quorumName;

        /**
         * name of a class implementing {@link com.hazelcast.cache.CacheMergePolicy}
         * that handles merging of values for this cache while recovering from
         * network partitioning
         */
        public java.lang.String mergePolicy;

        /**
         * when {@code true} disables invalidation events for per entry but
         * full-flush invalidation events are still enabled.
         */
        public boolean disablePerEntryInvalidationEvents;

        /**
         * partition lost listener configurations
         */
        public java.util.List<com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder> partitionLostListenerConfigs;

        /**
         * expiry policy factory class name. When configuring an expiry policy,
         * either this or {@ode timedExpiryPolicyFactoryConfig} should be configured.
         */
        public java.lang.String expiryPolicyFactoryClassName;

        /**
         * expiry policy factory with duration configuration
         */
        public com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig;

        /**
         * cache entry listeners configuration
         */
        public java.util.List<com.hazelcast.config.CacheSimpleEntryListenerConfig> cacheEntryListeners;

        /**
         * cache eviction configuration
         */
        public com.hazelcast.client.impl.protocol.task.dynamicconfig.EvictionConfigHolder evictionConfig;

        /**
         * reference to an existing WAN replication configuration
         */
        public com.hazelcast.config.WanReplicationRef wanReplicationRef;

        /**
         * Event Journal configuration
         */
        public com.hazelcast.config.EventJournalConfig eventJournalConfig;

        /**
         * hot restart configuration
         */
        public com.hazelcast.config.HotRestartConfig hotRestartConfig;
    }

    public static ClientMessage encodeRequest(java.lang.String name, java.lang.String keyType, java.lang.String valueType, boolean statisticsEnabled, boolean managementEnabled, boolean readThrough, boolean writeThrough, java.lang.String cacheLoaderFactory, java.lang.String cacheWriterFactory, java.lang.String cacheLoader, java.lang.String cacheWriter, int backupCount, int asyncBackupCount, java.lang.String inMemoryFormat, java.lang.String quorumName, java.lang.String mergePolicy, boolean disablePerEntryInvalidationEvents, java.util.Collection<com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder> partitionLostListenerConfigs, java.lang.String expiryPolicyFactoryClassName, com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig, java.util.Collection<com.hazelcast.config.CacheSimpleEntryListenerConfig> cacheEntryListeners, com.hazelcast.client.impl.protocol.task.dynamicconfig.EvictionConfigHolder evictionConfig, com.hazelcast.config.WanReplicationRef wanReplicationRef, com.hazelcast.config.EventJournalConfig eventJournalConfig, com.hazelcast.config.HotRestartConfig hotRestartConfig) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("DynamicConfig.AddCacheConfig");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeBoolean(initialFrame.content, REQUEST_STATISTICS_ENABLED_FIELD_OFFSET, statisticsEnabled);
        encodeBoolean(initialFrame.content, REQUEST_MANAGEMENT_ENABLED_FIELD_OFFSET, managementEnabled);
        encodeBoolean(initialFrame.content, REQUEST_READ_THROUGH_FIELD_OFFSET, readThrough);
        encodeBoolean(initialFrame.content, REQUEST_WRITE_THROUGH_FIELD_OFFSET, writeThrough);
        encodeInt(initialFrame.content, REQUEST_BACKUP_COUNT_FIELD_OFFSET, backupCount);
        encodeInt(initialFrame.content, REQUEST_ASYNC_BACKUP_COUNT_FIELD_OFFSET, asyncBackupCount);
        encodeBoolean(initialFrame.content, REQUEST_DISABLE_PER_ENTRY_INVALIDATION_EVENTS_FIELD_OFFSET, disablePerEntryInvalidationEvents);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        CodecUtil.encodeNullable(clientMessage, keyType, StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, valueType, StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, cacheLoaderFactory, StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, cacheWriterFactory, StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, cacheLoader, StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, cacheWriter, StringCodec::encode);
        StringCodec.encode(clientMessage, inMemoryFormat);
        CodecUtil.encodeNullable(clientMessage, quorumName, StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, mergePolicy, StringCodec::encode);
        ListMultiFrameCodec.encodeNullable(clientMessage, partitionLostListenerConfigs, ListenerConfigHolderCodec::encode);
        CodecUtil.encodeNullable(clientMessage, expiryPolicyFactoryClassName, StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, timedExpiryPolicyFactoryConfig, TimedExpiryPolicyFactoryConfigCodec::encode);
        ListMultiFrameCodec.encodeNullable(clientMessage, cacheEntryListeners, CacheSimpleEntryListenerConfigCodec::encode);
        CodecUtil.encodeNullable(clientMessage, evictionConfig, EvictionConfigHolderCodec::encode);
        CodecUtil.encodeNullable(clientMessage, wanReplicationRef, WanReplicationRefCodec::encode);
        CodecUtil.encodeNullable(clientMessage, eventJournalConfig, EventJournalConfigCodec::encode);
        CodecUtil.encodeNullable(clientMessage, hotRestartConfig, HotRestartConfigCodec::encode);
        return clientMessage;
    }

    public static DynamicConfigAddCacheConfigCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.statisticsEnabled = decodeBoolean(initialFrame.content, REQUEST_STATISTICS_ENABLED_FIELD_OFFSET);
        request.managementEnabled = decodeBoolean(initialFrame.content, REQUEST_MANAGEMENT_ENABLED_FIELD_OFFSET);
        request.readThrough = decodeBoolean(initialFrame.content, REQUEST_READ_THROUGH_FIELD_OFFSET);
        request.writeThrough = decodeBoolean(initialFrame.content, REQUEST_WRITE_THROUGH_FIELD_OFFSET);
        request.backupCount = decodeInt(initialFrame.content, REQUEST_BACKUP_COUNT_FIELD_OFFSET);
        request.asyncBackupCount = decodeInt(initialFrame.content, REQUEST_ASYNC_BACKUP_COUNT_FIELD_OFFSET);
        request.disablePerEntryInvalidationEvents = decodeBoolean(initialFrame.content, REQUEST_DISABLE_PER_ENTRY_INVALIDATION_EVENTS_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        request.keyType = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        request.valueType = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        request.cacheLoaderFactory = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        request.cacheWriterFactory = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        request.cacheLoader = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        request.cacheWriter = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        request.inMemoryFormat = StringCodec.decode(iterator);
        request.quorumName = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        request.mergePolicy = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        request.partitionLostListenerConfigs = ListMultiFrameCodec.decodeNullable(iterator, ListenerConfigHolderCodec::decode);
        request.expiryPolicyFactoryClassName = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        request.timedExpiryPolicyFactoryConfig = CodecUtil.decodeNullable(iterator, TimedExpiryPolicyFactoryConfigCodec::decode);
        request.cacheEntryListeners = ListMultiFrameCodec.decodeNullable(iterator, CacheSimpleEntryListenerConfigCodec::decode);
        request.evictionConfig = CodecUtil.decodeNullable(iterator, EvictionConfigHolderCodec::decode);
        request.wanReplicationRef = CodecUtil.decodeNullable(iterator, WanReplicationRefCodec::decode);
        request.eventJournalConfig = CodecUtil.decodeNullable(iterator, EventJournalConfigCodec::decode);
        request.hotRestartConfig = CodecUtil.decodeNullable(iterator, HotRestartConfigCodec::decode);
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

    public static DynamicConfigAddCacheConfigCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        ResponseParameters response = new ResponseParameters();
        //empty initial frame
        iterator.next();
        return response;
    }

}
