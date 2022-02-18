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

@Generated("58aef624b0124d9795fae1f10417b9e6")
public final class DistributedObjectInfoCodec {

    private DistributedObjectInfoCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.client.impl.client.DistributedObjectInfo distributedObjectInfo) {
        clientMessage.add(BEGIN_FRAME.copy());

        StringCodec.encode(clientMessage, distributedObjectInfo.getServiceName());
        StringCodec.encode(clientMessage, distributedObjectInfo.getName());

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.client.impl.client.DistributedObjectInfo decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        java.lang.String serviceName = StringCodec.decode(iterator);
        java.lang.String name = StringCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.client.DistributedObjectInfo(serviceName, name);
    }
}
