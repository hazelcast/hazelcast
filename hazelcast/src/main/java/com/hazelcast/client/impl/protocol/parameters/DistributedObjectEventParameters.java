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

package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageType;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.core.DistributedObjectEvent;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class DistributedObjectEventParameters {

    public static final ClientMessageType TYPE = ClientMessageType.DESTROY_PROXY_REQUEST;
    public String name;
    public String serviceName;
    public DistributedObjectEvent.EventType eventType;

    private DistributedObjectEventParameters(ClientMessage flyweight) {
        name = flyweight.getStringUtf8();
        serviceName = flyweight.getStringUtf8();
        eventType = DistributedObjectEvent.EventType.valueOf(flyweight.getStringUtf8());
    }

    public static DistributedObjectEventParameters decode(ClientMessage flyweight) {
        return new DistributedObjectEventParameters(flyweight);
    }

    public static ClientMessage encode(String name, String serviceName, DistributedObjectEvent.EventType eventType) {
        final int requiredDataSize = calculateDataSize(name, serviceName, eventType);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.set(name).set(serviceName).set(eventType.name());
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static int calculateDataSize(String name, String serviceName, DistributedObjectEvent.EventType eventType) {
        return ClientMessage.HEADER_SIZE
                + ParameterUtil.calculateStringDataSize(name)
                + ParameterUtil.calculateStringDataSize(serviceName)
                + ParameterUtil.calculateStringDataSize(eventType.name());
    }
}

