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

@Generated("7f78e8a7c2f2b051057bbfc1b17c012b")
public final class DurationConfigCodec {
    private static final int DURATION_AMOUNT_FIELD_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = DURATION_AMOUNT_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private DurationConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig durationConfig) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeLong(initialFrame.content, DURATION_AMOUNT_FIELD_OFFSET, durationConfig.getDurationAmount());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, durationConfig.getTimeUnit());

        clientMessage.add(END_FRAME);
    }

    public static com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        long durationAmount = decodeLong(initialFrame.content, DURATION_AMOUNT_FIELD_OFFSET);

        String timeUnit = StringCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createDurationConfig(durationAmount, timeUnit);
    }
}
