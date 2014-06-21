package com.hazelcast.spi.impl;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * Portable map event.
 *
 * @see com.hazelcast.core.EntryEventType#EVICT_ALL
 */
public class PortableMapEventData extends AbstractPortableEventData {

    private int numberOfEntriesAffected;

    public PortableMapEventData() {
    }

    public PortableMapEventData(EntryEventType eventType, String uuid, int numberOfEntriesAffected) {
        super(eventType, uuid);
        this.numberOfEntriesAffected = numberOfEntriesAffected;
    }

    public int getNumberOfEntriesAffected() {
        return numberOfEntriesAffected;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
        writer.writeInt("n", numberOfEntriesAffected);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        super.readPortable(reader);
        numberOfEntriesAffected = reader.readInt("n");
    }

    @Override
    public int getFactoryId() {
        return SpiPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return SpiPortableHook.MAP_EVENT;
    }
}
