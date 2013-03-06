/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * @ali 1/9/13
 */
public class CollectionEvent implements DataSerializable {

    CollectionProxyId proxyId;

    Data key;

    Data value;

    EntryEventType eventType;

    Address caller;

    public CollectionEvent() {
    }

    public CollectionEvent(CollectionProxyId proxyId, Data key, Data value, EntryEventType eventType, Address caller) {
        this.proxyId = proxyId;
        this.key = key;
        this.value = value;
        this.eventType = eventType;
        this.caller = caller;
    }

    public CollectionProxyId getProxyId() {
        return proxyId;
    }

    public Data getValue() {
        return value;
    }

    public EntryEventType getEventType() {
        return eventType;
    }

    public Address getCaller() {
        return caller;
    }

    public Data getKey() {
        return key;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        proxyId.writeData(out);
        key.writeData(out);
        IOUtil.writeNullableData(out, value);
        out.writeInt(eventType.getType());
        caller.writeData(out);
    }

    public void readData(ObjectDataInput in) throws IOException {
        proxyId = new CollectionProxyId();
        proxyId.readData(in);
        key = IOUtil.readData(in);
        value = IOUtil.readNullableData(in);
        eventType = EntryEventType.getByType(in.readInt());
        caller = new Address();
        caller.readData(in);
    }
}
