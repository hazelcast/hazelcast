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
import com.hazelcast.client.impl.protocol.util.BitUtil;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.serialization.Data;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class TopicEventParameters {

    public static final ClientMessageType TYPE = ClientMessageType.ADD_LISTENER_RESULT;
    public Data message;
    public long publishTime;
    public String uuid;


    private TopicEventParameters(ClientMessage flyweight) {
        message = flyweight.getData();
        publishTime = flyweight.getLong();
        uuid = flyweight.getStringUtf8();
    }

    public static TopicEventParameters decode(ClientMessage flyweight) {
        return new TopicEventParameters(flyweight);
    }

    public static ClientMessage encode(Data message, long publishTime, String uuid) {
        final int requiredDataSize = calculateDataSize(message, publishTime, uuid);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.ensureCapacity(requiredDataSize);
        clientMessage.set(message).set(publishTime).set(uuid);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    /**
     * sample data size estimation
     *
     * @return size
     */
    public static int calculateDataSize(Data message, long publishTime, String uuid) {
        return ClientMessage.HEADER_SIZE
                + ParameterUtil.calculateDataSize(message)
                + BitUtil.SIZE_OF_LONG
                + ParameterUtil.calculateStringDataSize(uuid);
    }
}
