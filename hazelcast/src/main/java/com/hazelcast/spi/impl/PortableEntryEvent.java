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

/**
 * @ali 5/23/13
 */
public class PortableEntryEvent implements Portable {

    private Data key;
    private Data value;
    private Data oldValue;
    private EntryEventType eventType;
    private String uuid;

    public PortableEntryEvent() {
    }

    public PortableEntryEvent(Data key, Data value, Data oldValue, EntryEventType eventType, String uuid) {
        this.key = key;
        this.value = value;
        this.oldValue = oldValue;
        this.eventType = eventType;
        this.uuid = uuid;
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

    public int getFactoryId() {
        return SpiPortableHook.ID;
    }

    public int getClassId() {
        return SpiPortableHook.ENTRY_EVENT;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("e", eventType.getType());
        writer.writeUTF("u", uuid);
        final ObjectDataOutput out = writer.getRawDataOutput();
        key.writeData(out);
        IOUtil.writeNullableData(out, value);
        IOUtil.writeNullableData(out, oldValue);
    }

    public void readPortable(PortableReader reader) throws IOException {
        eventType = EntryEventType.getByType(reader.readInt("e"));
        uuid = reader.readUTF("u");
        final ObjectDataInput in = reader.getRawDataInput();
        key = new Data();
        key.readData(in);
        value = IOUtil.readNullableData(in);
        oldValue = IOUtil.readNullableData(in);
    }
}
