/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.client.DistributedObjectInfo;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;

public final class DistributedObjectInfoCodec {

    private DistributedObjectInfoCodec() {
    }

    public static DistributedObjectInfo decode(ClientMessage clientMessage) {
        String serviceName = clientMessage.getStringUtf8();
        String name = clientMessage.getStringUtf8();
        return new DistributedObjectInfo(serviceName, name);
    }

    public static void encode(DistributedObjectInfo info, ClientMessage clientMessage) {
        clientMessage.
                set(info.getServiceName()).
                set(info.getName());
    }

    public static int calculateDataSize(DistributedObjectInfo info) {
        return ParameterUtil.calculateStringDataSize(info.getServiceName())
                + ParameterUtil.calculateStringDataSize(info.getName());
    }
}

