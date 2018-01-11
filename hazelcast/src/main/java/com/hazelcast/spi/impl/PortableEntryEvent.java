/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
    private Data mergingValue;
    private EntryEventType eventType;
    private String uuid;
    private int numberOfAffectedEntries = 1;

    public PortableEntryEvent() {
    }

    public PortableEntryEvent(Data key, Data value, Data oldValue, Data mergingValue, EntryEventType eventType, String uuid) {
        this.key = key;
        this.value = value;
        this.oldValue = oldValue;
        this.mergingValue = mergingValue;
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

    public Data getMergingValue() {
        return mergingValue;
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
        // Map Event and Entry Event is merged to one event, because when cpp client get response
        // from node, it first creates the class then fills the class what comes from wire. Currently
        // it can not handle more than one type response.
        writer.writeInt("e", eventType.getType());
        writer.writeUTF("u", uuid);
        writer.writeInt("n", numberOfAffectedEntries);

        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeData(key);
        out.writeData(value);
        out.writeData(oldValue);
        out.writeData(mergingValue);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        eventType = EntryEventType.getByType(reader.readInt("e"));
        uuid = reader.readUTF("u");
        numberOfAffectedEntries = reader.readInt("n");

        ObjectDataInput in = reader.getRawDataInput();
        key = in.readData();
        value = in.readData();
        oldValue = in.readData();
        mergingValue = in.readData();
    }
}
