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

package com.hazelcast.client.impl.protocol.codec.custom;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

@Generated("940e5ecc325b3cd073644fa501289247")
public final class CacheConfigHolderCodec {
    private static final int BACKUP_COUNT_FIELD_OFFSET = 0;
    private static final int ASYNC_BACKUP_COUNT_FIELD_OFFSET = BACKUP_COUNT_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int READ_THROUGH_FIELD_OFFSET = ASYNC_BACKUP_COUNT_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int WRITE_THROUGH_FIELD_OFFSET = READ_THROUGH_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int STORE_BY_VALUE_FIELD_OFFSET = WRITE_THROUGH_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int MANAGEMENT_ENABLED_FIELD_OFFSET = STORE_BY_VALUE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int STATISTICS_ENABLED_FIELD_OFFSET = MANAGEMENT_ENABLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int DISABLE_PER_ENTRY_INVALIDATION_EVENTS_FIELD_OFFSET = STATISTICS_ENABLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = DISABLE_PER_ENTRY_INVALIDATION_EVENTS_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private CacheConfigHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.client.impl.protocol.codec.holder.CacheConfigHolder cacheConfigHolder) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, BACKUP_COUNT_FIELD_OFFSET, cacheConfigHolder.getBackupCount());
        encodeInt(initialFrame.content, ASYNC_BACKUP_COUNT_FIELD_OFFSET, cacheConfigHolder.getAsyncBackupCount());
        encodeBoolean(initialFrame.content, READ_THROUGH_FIELD_OFFSET, cacheConfigHolder.isReadThrough());
        encodeBoolean(initialFrame.content, WRITE_THROUGH_FIELD_OFFSET, cacheConfigHolder.isWriteThrough());
        encodeBoolean(initialFrame.content, STORE_BY_VALUE_FIELD_OFFSET, cacheConfigHolder.isStoreByValue());
        encodeBoolean(initialFrame.content, MANAGEMENT_ENABLED_FIELD_OFFSET, cacheConfigHolder.isManagementEnabled());
        encodeBoolean(initialFrame.content, STATISTICS_ENABLED_FIELD_OFFSET, cacheConfigHolder.isStatisticsEnabled());
        encodeBoolean(initialFrame.content, DISABLE_PER_ENTRY_INVALIDATION_EVENTS_FIELD_OFFSET, cacheConfigHolder.isDisablePerEntryInvalidationEvents());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, cacheConfigHolder.getName());
        CodecUtil.encodeNullable(clientMessage, cacheConfigHolder.getManagerPrefix(), StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, cacheConfigHolder.getUriString(), StringCodec::encode);
        StringCodec.encode(clientMessage, cacheConfigHolder.getInMemoryFormat());
        EvictionConfigHolderCodec.encode(clientMessage, cacheConfigHolder.getEvictionConfigHolder());
        CodecUtil.encodeNullable(clientMessage, cacheConfigHolder.getWanReplicationRef(), WanReplicationRefCodec::encode);
        StringCodec.encode(clientMessage, cacheConfigHolder.getKeyClassName());
        StringCodec.encode(clientMessage, cacheConfigHolder.getValueClassName());
        CodecUtil.encodeNullable(clientMessage, cacheConfigHolder.getCacheLoaderFactory(), DataCodec::encode);
        CodecUtil.encodeNullable(clientMessage, cacheConfigHolder.getCacheWriterFactory(), DataCodec::encode);
        DataCodec.encode(clientMessage, cacheConfigHolder.getExpiryPolicyFactory());
        CodecUtil.encodeNullable(clientMessage, cacheConfigHolder.getHotRestartConfig(), HotRestartConfigCodec::encode);
        CodecUtil.encodeNullable(clientMessage, cacheConfigHolder.getEventJournalConfig(), EventJournalConfigCodec::encode);
        CodecUtil.encodeNullable(clientMessage, cacheConfigHolder.getSplitBrainProtectionName(), StringCodec::encode);
        ListMultiFrameCodec.encodeNullable(clientMessage, cacheConfigHolder.getListenerConfigurations(), DataCodec::encode);
        MergePolicyConfigCodec.encode(clientMessage, cacheConfigHolder.getMergePolicyConfig());
        ListMultiFrameCodec.encodeNullable(clientMessage, cacheConfigHolder.getCachePartitionLostListenerConfigs(), ListenerConfigHolderCodec::encode);
        CodecUtil.encodeNullable(clientMessage, cacheConfigHolder.getMerkleTreeConfig(), MerkleTreeConfigCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.client.impl.protocol.codec.holder.CacheConfigHolder decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int backupCount = decodeInt(initialFrame.content, BACKUP_COUNT_FIELD_OFFSET);
        int asyncBackupCount = decodeInt(initialFrame.content, ASYNC_BACKUP_COUNT_FIELD_OFFSET);
        boolean readThrough = decodeBoolean(initialFrame.content, READ_THROUGH_FIELD_OFFSET);
        boolean writeThrough = decodeBoolean(initialFrame.content, WRITE_THROUGH_FIELD_OFFSET);
        boolean storeByValue = decodeBoolean(initialFrame.content, STORE_BY_VALUE_FIELD_OFFSET);
        boolean managementEnabled = decodeBoolean(initialFrame.content, MANAGEMENT_ENABLED_FIELD_OFFSET);
        boolean statisticsEnabled = decodeBoolean(initialFrame.content, STATISTICS_ENABLED_FIELD_OFFSET);
        boolean disablePerEntryInvalidationEvents = decodeBoolean(initialFrame.content, DISABLE_PER_ENTRY_INVALIDATION_EVENTS_FIELD_OFFSET);

        java.lang.String name = StringCodec.decode(iterator);
        java.lang.String managerPrefix = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        java.lang.String uriString = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        java.lang.String inMemoryFormat = StringCodec.decode(iterator);
        com.hazelcast.client.impl.protocol.task.dynamicconfig.EvictionConfigHolder evictionConfigHolder = EvictionConfigHolderCodec.decode(iterator);
        com.hazelcast.config.WanReplicationRef wanReplicationRef = CodecUtil.decodeNullable(iterator, WanReplicationRefCodec::decode);
        java.lang.String keyClassName = StringCodec.decode(iterator);
        java.lang.String valueClassName = StringCodec.decode(iterator);
        com.hazelcast.internal.serialization.Data cacheLoaderFactory = CodecUtil.decodeNullable(iterator, DataCodec::decode);
        com.hazelcast.internal.serialization.Data cacheWriterFactory = CodecUtil.decodeNullable(iterator, DataCodec::decode);
        com.hazelcast.internal.serialization.Data expiryPolicyFactory = DataCodec.decode(iterator);
        com.hazelcast.config.HotRestartConfig hotRestartConfig = CodecUtil.decodeNullable(iterator, HotRestartConfigCodec::decode);
        com.hazelcast.config.EventJournalConfig eventJournalConfig = CodecUtil.decodeNullable(iterator, EventJournalConfigCodec::decode);
        java.lang.String splitBrainProtectionName = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        java.util.List<com.hazelcast.internal.serialization.Data> listenerConfigurations = ListMultiFrameCodec.decodeNullable(iterator, DataCodec::decode);
        com.hazelcast.config.MergePolicyConfig mergePolicyConfig = MergePolicyConfigCodec.decode(iterator);
        java.util.List<com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder> cachePartitionLostListenerConfigs = ListMultiFrameCodec.decodeNullable(iterator, ListenerConfigHolderCodec::decode);
        boolean isMerkleTreeConfigExists = false;
        com.hazelcast.config.MerkleTreeConfig merkleTreeConfig = null;
        if (!iterator.peekNext().isEndFrame()) {
            merkleTreeConfig = CodecUtil.decodeNullable(iterator, MerkleTreeConfigCodec::decode);
            isMerkleTreeConfigExists = true;
        }

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.protocol.codec.holder.CacheConfigHolder(name, managerPrefix, uriString, backupCount, asyncBackupCount, inMemoryFormat, evictionConfigHolder, wanReplicationRef, keyClassName, valueClassName, cacheLoaderFactory, cacheWriterFactory, expiryPolicyFactory, readThrough, writeThrough, storeByValue, managementEnabled, statisticsEnabled, hotRestartConfig, eventJournalConfig, splitBrainProtectionName, listenerConfigurations, mergePolicyConfig, disablePerEntryInvalidationEvents, cachePartitionLostListenerConfigs, isMerkleTreeConfigExists, merkleTreeConfig);
    }
}
