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

package com.hazelcast.collection.impl.collection;

import com.hazelcast.core.ItemEventType;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class CollectionEvent implements IdentifiedDataSerializable {

    private String name;
    private Data data;
    private ItemEventType eventType;
    private Address caller;

    public CollectionEvent() {
    }

    public CollectionEvent(String name, Data data, ItemEventType eventType, Address caller) {
        this.name = name;
        this.data = data;
        this.eventType = eventType;
        this.caller = caller;
    }

    public String getName() {
        return name;
    }

    public Data getData() {
        return data;
    }

    public ItemEventType getEventType() {
        return eventType;
    }

    public Address getCaller() {
        return caller;
    }

    @Override
    public int getFactoryId() {
        return CollectionDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.COLLECTION_EVENT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeInt(eventType.getType());
        caller.writeData(out);
        IOUtil.writeData(out, data);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        eventType = ItemEventType.getByType(in.readInt());
        caller = new Address();
        caller.readData(in);
        data = IOUtil.readData(in);
    }

}
