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

package com.hazelcast.client.impl.protocol.codec.custom;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

@SuppressWarnings("unused")
@Generated("0a591d0143287996afdfab0b1df71ce3")
public final class WanSyncConfigCodec {
    private static final int CONSISTENCY_CHECK_STRATEGY_FIELD_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = CONSISTENCY_CHECK_STRATEGY_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private WanSyncConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.client.impl.protocol.codec.holder.WanSyncConfigHolder wanSyncConfig) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeByte(initialFrame.content, CONSISTENCY_CHECK_STRATEGY_FIELD_OFFSET, wanSyncConfig.getConsistencyCheckStrategy());
        clientMessage.add(initialFrame);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.client.impl.protocol.codec.holder.WanSyncConfigHolder decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        byte consistencyCheckStrategy = decodeByte(initialFrame.content, CONSISTENCY_CHECK_STRATEGY_FIELD_OFFSET);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.protocol.codec.holder.WanSyncConfigHolder(consistencyCheckStrategy);
    }
}
