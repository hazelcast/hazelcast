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

import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import java.io.IOException;

public class CollectionEvent implements IdentifiedDataSerializable {

    String name;

    Data data;

    ItemEventType eventType;

    Address caller;

    public CollectionEvent() {
    }

    public CollectionEvent(String name, Data data, ItemEventType eventType, Address caller) {
        this.name = name;
        this.data = data;
        this.eventType = eventType;
        this.caller = caller;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(eventType.getType());
        caller.writeData(out);
        IOUtil.writeNullableData(out, data);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        eventType = ItemEventType.getByType(in.readInt());
        caller = new Address();
        caller.readData(in);
        data = IOUtil.readNullableData(in);
    }

    @Override
    public int getFactoryId() {
        return CollectionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_EVENT;
    }
}
