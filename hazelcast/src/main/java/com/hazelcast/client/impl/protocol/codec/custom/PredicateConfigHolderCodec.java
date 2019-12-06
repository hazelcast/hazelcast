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
import com.hazelcast.internal.serialization.Data;

import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.ClientMessage.*;

@Generated("6e0f94fa7c1cbd2912b9490d1dac3b49")
public final class PredicateConfigHolderCodec {

    private PredicateConfigHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.client.impl.protocol.task.dynamicconfig.PredicateConfigHolder predicateConfigHolder) {
        clientMessage.add(BEGIN_FRAME.copy());

        CodecUtil.encodeNullable(clientMessage, predicateConfigHolder.getClassName(), StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, predicateConfigHolder.getSql(), StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, predicateConfigHolder.getImplementation(), DataCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.client.impl.protocol.task.dynamicconfig.PredicateConfigHolder decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        java.lang.String className = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        java.lang.String sql = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        Data implementation = CodecUtil.decodeNullable(iterator, DataCodec::decode);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.protocol.task.dynamicconfig.PredicateConfigHolder(className, sql, implementation);
    }
}
