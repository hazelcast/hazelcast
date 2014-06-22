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

package com.hazelcast.spi.impl;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class PortableEntryEvent implements Portable {

    private Data key;
    private Data value;
    private Data oldValue;
    private EntryEventType eventType;
    private String uuid;
    private int numberOfAffectedEntries = 1;

    public PortableEntryEvent() {
    }

    public PortableEntryEvent(Data key, Data value, Data oldValue, EntryEventType eventType, String uuid) {
        this.key = key;
        this.value = value;
        this.oldValue = oldValue;
        this.eventType = eventType;
        this.uuid = uuid;
    }

    public PortableEntryEvent(EntryEventType eventType, String uuid, int numberOfAffectedEntries) {
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

    public String getUuid() {
        return uuid;
    }

    public int getNumberOfAffectedEntries() {
        return numberOfAffectedEntries;
    }

    @Override
    public int getFactoryId() {
        return SpiPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return SpiPortableHook.ENTRY_EVENT;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("e", eventType.getType());
        writer.writeUTF("u", uuid);
        writer.writeInt("n", numberOfAffectedEntries);
        final ObjectDataOutput out = writer.getRawDataOutput();
        IOUtil.writeNullableData(out, key);
        IOUtil.writeNullableData(out, value);
        IOUtil.writeNullableData(out, oldValue);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        eventType = EntryEventType.getByType(reader.readInt("e"));
        uuid = reader.readUTF("u");
        numberOfAffectedEntries = reader.readInt("n");
        final ObjectDataInput in = reader.getRawDataInput();
        key = IOUtil.readNullableData(in);
        value = IOUtil.readNullableData(in);
        oldValue = IOUtil.readNullableData(in);
    }
}
