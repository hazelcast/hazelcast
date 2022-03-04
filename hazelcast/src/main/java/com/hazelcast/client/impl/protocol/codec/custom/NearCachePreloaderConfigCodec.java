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

@Generated("40171a6b645099d7520696a8224ce1a3")
public final class NearCachePreloaderConfigCodec {
    private static final int ENABLED_FIELD_OFFSET = 0;
    private static final int STORE_INITIAL_DELAY_SECONDS_FIELD_OFFSET = ENABLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int STORE_INTERVAL_SECONDS_FIELD_OFFSET = STORE_INITIAL_DELAY_SECONDS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = STORE_INTERVAL_SECONDS_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private NearCachePreloaderConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.config.NearCachePreloaderConfig nearCachePreloaderConfig) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, ENABLED_FIELD_OFFSET, nearCachePreloaderConfig.isEnabled());
        encodeInt(initialFrame.content, STORE_INITIAL_DELAY_SECONDS_FIELD_OFFSET, nearCachePreloaderConfig.getStoreInitialDelaySeconds());
        encodeInt(initialFrame.content, STORE_INTERVAL_SECONDS_FIELD_OFFSET, nearCachePreloaderConfig.getStoreIntervalSeconds());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, nearCachePreloaderConfig.getDirectory());

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.config.NearCachePreloaderConfig decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean enabled = decodeBoolean(initialFrame.content, ENABLED_FIELD_OFFSET);
        int storeInitialDelaySeconds = decodeInt(initialFrame.content, STORE_INITIAL_DELAY_SECONDS_FIELD_OFFSET);
        int storeIntervalSeconds = decodeInt(initialFrame.content, STORE_INTERVAL_SECONDS_FIELD_OFFSET);

        java.lang.String directory = StringCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createNearCachePreloaderConfig(enabled, directory, storeInitialDelaySeconds, storeIntervalSeconds);
    }
}
