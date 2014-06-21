package com.hazelcast.map;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.impl.EventData;

import java.io.IOException;

/**
 * Abstract event data.
 */
abstract class AbstractMapEventData implements EventData, DataSerializable {

    private String source;
    private String mapName;
    private Address caller;
    private int eventType;

    public AbstractMapEventData() {
    }

    public AbstractMapEventData(String source, String mapName, Address caller, int eventType) {
        this.source = source;
        this.mapName = mapName;
        this.caller = caller;
        this.eventType = eventType;
    }

    public String getSource() {
        return source;
    }

    public String getMapName() {
        return mapName;
    }

    public Address getCaller() {
        return caller;
    }

    public int getEventType() {
        return eventType;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(source);
        out.writeUTF(mapName);
        out.writeObject(caller);
        out.writeInt(eventType);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        source = in.readUTF();
        mapName = in.readUTF();
        caller = in.readObject();
        eventType = in.readInt();
    }

    @Override
    public String toString() {
        return "source='" + source + '\''
                + ", mapName='" + mapName + '\''
                + ", caller=" + caller
                + ", eventType=" + eventType;
    }
}
