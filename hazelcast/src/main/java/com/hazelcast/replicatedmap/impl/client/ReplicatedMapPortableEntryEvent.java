/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.client;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.util.UUID;

/**
 * Class to redirect entry listener events on the members to the listening client
 */
public class ReplicatedMapPortableEntryEvent implements Portable {

    private Data key;
    private Data value;
    private Data oldValue;
    private EntryEventType eventType;
    private UUID uuid;
    private int numberOfAffectedEntries;

    ReplicatedMapPortableEntryEvent() {
    }

    ReplicatedMapPortableEntryEvent(Data key, Data value, Data oldValue, EntryEventType eventType, UUID uuid) {
        this(key, value, oldValue, eventType, uuid, 0);
    }

    ReplicatedMapPortableEntryEvent(Data key, Data value, Data oldValue, EntryEventType eventType, UUID uuid,
                                    int numberOfAffectedEntries) {
        this.key = key;
        this.value = value;
        this.oldValue = oldValue;
        this.eventType = eventType;
        this.uuid = uuid;
        this.numberOfAffectedEntries = numberOfAffectedEntries;
    }

    public Data getKey() {
        return key;
    }

    public Data getValue() {
        return value;
    }

    public Data getOldValue() {
        return oldValue;
    }

    public EntryEventType getEventType() {
        return eventType;
    }

    public UUID getUuid() {
        return uuid;
    }

    public int getNumberOfAffectedEntries() {
        return numberOfAffectedEntries;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("e", eventType.getType());
        writer.writeLong("uHigh", uuid.getMostSignificantBits());
        writer.writeLong("uLow", uuid.getLeastSignificantBits());
        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeData(key);
        out.writeData(value);
        out.writeData(oldValue);
        out.writeInt(numberOfAffectedEntries);
    }

    public void readPortable(PortableReader reader) throws IOException {
        eventType = EntryEventType.getByType(reader.readInt("e"));
        uuid = new UUID(reader.readLong("uHigh"), reader.readLong("uLow"));
        ObjectDataInput in = reader.getRawDataInput();
        key = in.readData();
        value = in.readData();
        oldValue = in.readData();
        numberOfAffectedEntries = in.readInt();
    }

    public int getFactoryId() {
        return ReplicatedMapPortableHook.F_ID;
    }

    public int getClassId() {
        return ReplicatedMapPortableHook.MAP_ENTRY_EVENT;
    }
}
