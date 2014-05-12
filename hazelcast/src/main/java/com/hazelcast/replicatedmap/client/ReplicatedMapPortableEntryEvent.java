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

package com.hazelcast.replicatedmap.client;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * Class to redirect entry listener events on the members to the listening client
 */
public class ReplicatedMapPortableEntryEvent
        implements Portable {

    private Object key;
    private Object value;
    private Object oldValue;
    private EntryEventType eventType;
    private String uuid;

    ReplicatedMapPortableEntryEvent() {
    }

    ReplicatedMapPortableEntryEvent(Object key, Object value, Object oldValue, EntryEventType eventType, String uuid) {
        this.key = key;
        this.value = value;
        this.oldValue = oldValue;
        this.eventType = eventType;
        this.uuid = uuid;
    }

    public Object getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public Object getOldValue() {
        return oldValue;
    }

    public EntryEventType getEventType() {
        return eventType;
    }

    public String getUuid() {
        return uuid;
    }

    public void writePortable(PortableWriter writer)
            throws IOException {
        writer.writeInt("e", eventType.getType());
        writer.writeUTF("u", uuid);
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(key);
        out.writeObject(value);
        out.writeObject(oldValue);
    }

    public void readPortable(PortableReader reader)
            throws IOException {
        eventType = EntryEventType.getByType(reader.readInt("e"));
        uuid = reader.readUTF("u");
        final ObjectDataInput in = reader.getRawDataInput();
        key = in.readObject();
        value = in.readObject();
        oldValue = in.readObject();
    }

    public int getFactoryId() {
        return ReplicatedMapPortableHook.F_ID;
    }

    public int getClassId() {
        return ReplicatedMapPortableHook.MAP_ENTRY_EVENT;
    }

}
