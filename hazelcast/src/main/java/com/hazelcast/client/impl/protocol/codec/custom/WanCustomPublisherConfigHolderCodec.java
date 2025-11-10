/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
@Generated("20408531eaa77e98223bd2ff11c2e53c")
public final class WanCustomPublisherConfigHolderCodec {

    private WanCustomPublisherConfigHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.client.impl.protocol.codec.holder.WanCustomPublisherConfigHolder wanCustomPublisherConfigHolder) {
        clientMessage.add(BEGIN_FRAME.copy());

        CodecUtil.encodeNullable(clientMessage, wanCustomPublisherConfigHolder.getPublisherId(), StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, wanCustomPublisherConfigHolder.getClassName(), StringCodec::encode);
        DataCodec.encodeNullable(clientMessage, wanCustomPublisherConfigHolder.getImplementation());
        MapCodec.encode(clientMessage, wanCustomPublisherConfigHolder.getProperties(), StringCodec::encode, DataCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.client.impl.protocol.codec.holder.WanCustomPublisherConfigHolder decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        java.lang.String publisherId = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        java.lang.String className = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        com.hazelcast.internal.serialization.Data implementation = DataCodec.decodeNullable(iterator);
        java.util.Map<java.lang.String, com.hazelcast.internal.serialization.Data> properties = MapCodec.decode(iterator, StringCodec::decode, DataCodec::decode);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.protocol.codec.holder.WanCustomPublisherConfigHolder(publisherId, className, implementation, properties);
    }
}
