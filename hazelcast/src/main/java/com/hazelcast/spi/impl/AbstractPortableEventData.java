package com.hazelcast.spi.impl;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * Includes common parts of portable event.
 */
public abstract class AbstractPortableEventData implements Portable, EventData {

    private EntryEventType eventType;
    private String uuid;

    protected AbstractPortableEventData() {
    }

    protected AbstractPortableEventData(EntryEventType eventType, String uuid) {
        this.eventType = eventType;
        this.uuid = uuid;
    }

    public EntryEventType getEventType() {
        return eventType;
    }

    public String getUuid() {
        return uuid;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("e", eventType.getType());
        writer.writeUTF("u", uuid);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        eventType = EntryEventType.getByType(reader.readInt("e"));
        uuid = reader.readUTF("u");
    }
}

