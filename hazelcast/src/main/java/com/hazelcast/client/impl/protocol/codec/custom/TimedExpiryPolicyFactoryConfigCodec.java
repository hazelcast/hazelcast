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

@Generated("43c95a05726620bbc2dc22e3548f358e")
public final class TimedExpiryPolicyFactoryConfigCodec {
    private static final int EXPIRY_POLICY_TYPE_FIELD_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = EXPIRY_POLICY_TYPE_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private TimedExpiryPolicyFactoryConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, EXPIRY_POLICY_TYPE_FIELD_OFFSET, timedExpiryPolicyFactoryConfig.getExpiryPolicyType());
        clientMessage.add(initialFrame);

        DurationConfigCodec.encode(clientMessage, timedExpiryPolicyFactoryConfig.getDurationConfig());

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int expiryPolicyType = decodeInt(initialFrame.content, EXPIRY_POLICY_TYPE_FIELD_OFFSET);

        com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig durationConfig = DurationConfigCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createTimedExpiryPolicyFactoryConfig(expiryPolicyType, durationConfig);
    }
}
