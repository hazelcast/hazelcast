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

@Generated("dafaaf5475842a91db7a9469eff896be")
public final class AttributeConfigCodec {

    private AttributeConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.config.AttributeConfig attributeConfig) {
        clientMessage.add(BEGIN_FRAME.copy());

        StringCodec.encode(clientMessage, attributeConfig.getName());
        StringCodec.encode(clientMessage, attributeConfig.getExtractorClassName());

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.config.AttributeConfig decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        java.lang.String name = StringCodec.decode(iterator);
        java.lang.String extractorClassName = StringCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.config.AttributeConfig(name, extractorClassName);
    }
}
