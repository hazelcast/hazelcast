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
import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.serialization.Data;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class ItemEventParameters {

    public static final ClientMessageType TYPE = ClientMessageType.ADD_LISTENER_RESULT;
    public Data item;
    public String uuid;
    public ItemEventType eventType;

    private ItemEventParameters(ClientMessage flyweight) {
        item = flyweight.getData();
        uuid = flyweight.getStringUtf8();
        eventType = ItemEventType.getByType(flyweight.getInt());
    }

    public static ItemEventParameters decode(ClientMessage flyweight) {
        return new ItemEventParameters(flyweight);
    }

    public static ClientMessage encode(Data item, String uuid, ItemEventType eventType) {
        final int requiredDataSize = calculateDataSize(item, uuid, eventType.getType());
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.ensureCapacity(requiredDataSize);
        clientMessage.set(item).set(uuid).set(eventType.getType());
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    /**
     * sample data size estimation
     *
     * @return size
     */
    public static int calculateDataSize(Data item, String uuid, int eventType) {
        return ClientMessage.HEADER_SIZE
                + ParameterUtil.calculateDataSize(item)
                + ParameterUtil.calculateStringDataSize(uuid) + BitUtil.SIZE_OF_INT;
    }
}

