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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.core.ItemEventType;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Used for queue-wide events.
 */
public class QueueEvent implements IdentifiedDataSerializable {

    String name;
    Data data;
    ItemEventType eventType;
    Address caller;

    public QueueEvent() {
    }

    public QueueEvent(String name, Data data, ItemEventType eventType, Address caller) {
        this.name = name;
        this.data = data;
        this.eventType = eventType;
        this.caller = caller;
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

    @Override
    public int getFactoryId() {
        return QueueDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.QUEUE_EVENT;
    }

    public String getName() {
        return name;
    }

    public ItemEventType getEventType() {
        return eventType;
    }
}
