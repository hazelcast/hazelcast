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

package com.hazelcast.client.impl.protocol.codec.custom;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

@Generated("dc0aa970e2c40a30fe00714280ea342c")
public final class MapConfigHolderCodec {
    private static final int BACKUP_COUNT_FIELD_OFFSET = 0;
    private static final int ASYNC_BACKUP_COUNT_FIELD_OFFSET = BACKUP_COUNT_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int TIME_TO_LIVE_SECONDS_FIELD_OFFSET = ASYNC_BACKUP_COUNT_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int MAX_IDLE_SECONDS_FIELD_OFFSET = TIME_TO_LIVE_SECONDS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int MAX_SIZE_FIELD_OFFSET = MAX_IDLE_SECONDS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int READ_BACKUP_DATA_FIELD_OFFSET = MAX_SIZE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = READ_BACKUP_DATA_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private MapConfigHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.client.impl.protocol.codec.holder.MapConfigHolder mapConfigHolder) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, BACKUP_COUNT_FIELD_OFFSET, mapConfigHolder.getBackupCount());
        encodeInt(initialFrame.content, ASYNC_BACKUP_COUNT_FIELD_OFFSET, mapConfigHolder.getAsyncBackupCount());
        encodeInt(initialFrame.content, TIME_TO_LIVE_SECONDS_FIELD_OFFSET, mapConfigHolder.getTimeToLiveSeconds());
        encodeInt(initialFrame.content, MAX_IDLE_SECONDS_FIELD_OFFSET, mapConfigHolder.getMaxIdleSeconds());
        encodeInt(initialFrame.content, MAX_SIZE_FIELD_OFFSET, mapConfigHolder.getMaxSize());
        encodeBoolean(initialFrame.content, READ_BACKUP_DATA_FIELD_OFFSET, mapConfigHolder.isReadBackupData());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, mapConfigHolder.getInMemoryFormat());
        StringCodec.encode(clientMessage, mapConfigHolder.getMaxSizePolicy());
        StringCodec.encode(clientMessage, mapConfigHolder.getEvictionPolicy());
        StringCodec.encode(clientMessage, mapConfigHolder.getMergePolicy());

        clientMessage.add(END_FRAME);
    }

    public static com.hazelcast.client.impl.protocol.codec.holder.MapConfigHolder decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int backupCount = decodeInt(initialFrame.content, BACKUP_COUNT_FIELD_OFFSET);
        int asyncBackupCount = decodeInt(initialFrame.content, ASYNC_BACKUP_COUNT_FIELD_OFFSET);
        int timeToLiveSeconds = decodeInt(initialFrame.content, TIME_TO_LIVE_SECONDS_FIELD_OFFSET);
        int maxIdleSeconds = decodeInt(initialFrame.content, MAX_IDLE_SECONDS_FIELD_OFFSET);
        int maxSize = decodeInt(initialFrame.content, MAX_SIZE_FIELD_OFFSET);
        boolean readBackupData = decodeBoolean(initialFrame.content, READ_BACKUP_DATA_FIELD_OFFSET);

        java.lang.String inMemoryFormat = StringCodec.decode(iterator);
        java.lang.String maxSizePolicy = StringCodec.decode(iterator);
        java.lang.String evictionPolicy = StringCodec.decode(iterator);
        java.lang.String mergePolicy = StringCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.protocol.codec.holder.MapConfigHolder(inMemoryFormat, backupCount, asyncBackupCount, timeToLiveSeconds, maxIdleSeconds, maxSize, maxSizePolicy, readBackupData, evictionPolicy, mergePolicy);
    }
}
