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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.holder.CacheConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.EvictionConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.nio.serialization.Data;

import java.util.List;
import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.decodeNullable;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.encodeNullable;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeBoolean;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeInt;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeBoolean;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeInt;

public final class CacheConfigHolderCodec {
    private static final int BACKUP_COUNT_OFFSET = 0;
    private static final int ASYNC_BACKUP_COUNT_OFFSET = BACKUP_COUNT_OFFSET + Bits.INT_SIZE_IN_BYTES;
    private static final int IS_READ_THROUGH_OFFSET = ASYNC_BACKUP_COUNT_OFFSET + Bits.INT_SIZE_IN_BYTES;
    private static final int IS_WRITE_THROUGH_OFFSET = IS_READ_THROUGH_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;
    private static final int IS_STORE_BY_VALUE_OFFSET = IS_WRITE_THROUGH_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;
    private static final int IS_MANAGEMENT_ENABLED_OFFSET = IS_STORE_BY_VALUE_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;
    private static final int IS_STATISTICS_ENABLED_OFFSET = IS_MANAGEMENT_ENABLED_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;
    private static final int DISABLE_PER_ENTRY_INVALIDATION_EVENTS_OFFSET =
            IS_STATISTICS_ENABLED_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;

    private static final int INITIAL_FRAME_SIZE = DISABLE_PER_ENTRY_INVALIDATION_EVENTS_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;

    private CacheConfigHolderCodec() {
    }

    public static <K, V> void encode(ClientMessage clientMessage, CacheConfigHolder config) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, BACKUP_COUNT_OFFSET, config.getBackupCount());
        encodeInt(initialFrame.content, ASYNC_BACKUP_COUNT_OFFSET, config.getAsyncBackupCount());
        encodeBoolean(initialFrame.content, IS_READ_THROUGH_OFFSET, config.isReadThrough());
        encodeBoolean(initialFrame.content, IS_WRITE_THROUGH_OFFSET, config.isWriteThrough());
        encodeBoolean(initialFrame.content, IS_STORE_BY_VALUE_OFFSET, config.isStoreByValue());
        encodeBoolean(initialFrame.content, IS_MANAGEMENT_ENABLED_OFFSET, config.isManagementEnabled());
        encodeBoolean(initialFrame.content, IS_STATISTICS_ENABLED_OFFSET, config.isStatisticsEnabled());
        encodeBoolean(initialFrame.content, DISABLE_PER_ENTRY_INVALIDATION_EVENTS_OFFSET,
                config.isDisablePerEntryInvalidationEvents());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, config.getName());
        encodeNullable(clientMessage, config.getManagerPrefix(), StringCodec::encode);
        encodeNullable(clientMessage, config.getUriString(), StringCodec::encode);
        StringCodec.encode(clientMessage, config.getInMemoryFormat());

        EvictionConfigHolderCodec.encode(clientMessage, config.getEvictionConfigHolder());

        encodeNullable(clientMessage, config.getWanReplicationRef(), WanReplicationRefCodec::encode);
        StringCodec.encode(clientMessage, config.getKeyClassName());
        StringCodec.encode(clientMessage, config.getValueClassName());
        encodeNullable(clientMessage, config.getCacheLoaderFactory(), DataCodec::encode);
        encodeNullable(clientMessage, config.getCacheWriterFactory(), DataCodec::encode);
        encodeNullable(clientMessage, config.getExpiryPolicyFactory(), DataCodec::encode);
        encodeNullable(clientMessage, config.getHotRestartConfig(), HotRestartConfigCodec::encode);
        encodeNullable(clientMessage, config.getEventJournalConfig(), EventJournalConfigCodec::encode);
        encodeNullable(clientMessage, config.getSplitBrainProtectionName(), StringCodec::encode);
        ListMultiFrameCodec.encodeNullable(clientMessage, config.getListenerConfigurations(), DataCodec::encode);
        MergePolicyConfigCodec.encode(clientMessage, config.getMergePolicyConfig());
        ListMultiFrameCodec
                .encodeNullable(clientMessage, config.getCachePartitionLostListenerConfigs(), ListenerConfigHolderCodec::encode);

        clientMessage.add(END_FRAME);
    }

    public static CacheConfigHolder decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int backupCount = decodeInt(initialFrame.content, BACKUP_COUNT_OFFSET);
        int asyncBackupCount = decodeInt(initialFrame.content, ASYNC_BACKUP_COUNT_OFFSET);
        boolean isReadThrough = decodeBoolean(initialFrame.content, IS_READ_THROUGH_OFFSET);
        boolean isWriteThrough = decodeBoolean(initialFrame.content, IS_WRITE_THROUGH_OFFSET);
        boolean isStoreByValue = decodeBoolean(initialFrame.content, IS_STORE_BY_VALUE_OFFSET);
        boolean isManagementEnabled = decodeBoolean(initialFrame.content, IS_MANAGEMENT_ENABLED_OFFSET);
        boolean isStatisticsEnabled = decodeBoolean(initialFrame.content, IS_STATISTICS_ENABLED_OFFSET);
        boolean disablePerEntryInvalidationEvents = decodeBoolean(initialFrame.content,
                DISABLE_PER_ENTRY_INVALIDATION_EVENTS_OFFSET);

        String name = StringCodec.decode(iterator);
        String managerPrefix = decodeNullable(iterator, StringCodec::decode);
        String uriString = decodeNullable(iterator, StringCodec::decode);
        String inMemoryFormat = StringCodec.decode(iterator);

        EvictionConfigHolder evictionConfigHolder = EvictionConfigHolderCodec.decode(iterator);

        WanReplicationRef wanReplicationRef = decodeNullable(iterator, WanReplicationRefCodec::decode);

        String keyClassName = StringCodec.decode(iterator);
        String valueClassName = StringCodec.decode(iterator);

        Data cacheLoaderFactory = decodeNullable(iterator, DataCodec::decode);
        Data cacheWriterFactory = decodeNullable(iterator, DataCodec::decode);
        Data expiryPolicyFactory = decodeNullable(iterator, DataCodec::decode);

        HotRestartConfig hotRestartConfig = decodeNullable(iterator, HotRestartConfigCodec::decode);
        EventJournalConfig eventJournalConfig = decodeNullable(iterator, EventJournalConfigCodec::decode);
        String splitBrainProtectionName = decodeNullable(iterator, StringCodec::decode);
        List<Data> listenerConfigurations = ListMultiFrameCodec.decodeNullable(iterator, DataCodec::decode);
        MergePolicyConfig mergePolicyConfig = MergePolicyConfigCodec.decode(iterator);
        List<ListenerConfigHolder> cachePartitionLostListenerConfigs = ListMultiFrameCodec
                .decodeNullable(iterator, ListenerConfigHolderCodec::decode);

        return new CacheConfigHolder(name, managerPrefix, uriString, backupCount, asyncBackupCount, inMemoryFormat,
                evictionConfigHolder, wanReplicationRef, keyClassName, valueClassName, cacheLoaderFactory, cacheWriterFactory,
                expiryPolicyFactory, isReadThrough, isWriteThrough, isStoreByValue, isManagementEnabled, isStatisticsEnabled,
                hotRestartConfig, eventJournalConfig, splitBrainProtectionName, listenerConfigurations, mergePolicyConfig,
                disablePerEntryInvalidationEvents, cachePartitionLostListenerConfigs);
    }
}
