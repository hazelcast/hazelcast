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

/**
 * EntryEventParameters
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class AddEntryListenerEventParameters {

    public static final ClientMessageType TYPE = ClientMessageType.ADD_ENTRY_LISTENER_EVENT;
    public Data key;
    public Data value;
    public Data oldValue;
    public Data mergingValue;
    public int eventType;
    public String uuid;
    public int numberOfAffectedEntries = 1;

    private AddEntryListenerEventParameters(ClientMessage flyweight) {
        key = flyweight.getData();
        value = flyweight.getData();
        oldValue = flyweight.getData();
        mergingValue = flyweight.getData();
        eventType = flyweight.getInt();
        uuid = flyweight.getStringUtf8();
        numberOfAffectedEntries = flyweight.getInt();
    }

    public static AddEntryListenerEventParameters decode(ClientMessage flyweight) {
        return new AddEntryListenerEventParameters(flyweight);
    }

    public static ClientMessage encode(Data key, Data value, Data oldValue, Data mergingValue, int eventType, String uuid, int numberOfAffectedEntries) {
        final int requiredDataSize = calculateDataSize(key, value, oldValue, mergingValue, eventType, uuid, numberOfAffectedEntries);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.ensureCapacity(requiredDataSize);
        clientMessage.set(key).set(value).set(oldValue).set(mergingValue).set(eventType).set(uuid).set(numberOfAffectedEntries);
        clientMessage.setFlags(ClientMessage.LISTENER_EVENT_FLAG);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    /**
     * sample data size estimation
     *
     * @return size
     */
    public static int calculateDataSize(Data key, Data value, Data oldValue, Data mergingValue, int eventType, String uuid, int numberOfAffectedEntries) {
        return ClientMessage.HEADER_SIZE
                + ParameterUtil.calculateDataSize(key)
                + ParameterUtil.calculateDataSize(value)
                + ParameterUtil.calculateDataSize(oldValue)
                + ParameterUtil.calculateDataSize(mergingValue)
                + BitUtil.SIZE_OF_INT//eventType
                + ParameterUtil.calculateStringDataSize(uuid)
                + BitUtil.SIZE_OF_INT;//numberOfAffectedEntries
    }

}
