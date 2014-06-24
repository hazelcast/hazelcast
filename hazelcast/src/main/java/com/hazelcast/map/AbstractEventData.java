package com.hazelcast.map;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Abstract event data.
 */
abstract class AbstractEventData implements EventData {

    private String source;
    private String mapName;
    private Address caller;
    private int eventType;

    public AbstractEventData() {
    }

    public AbstractEventData(String source, String mapName, Address caller, int eventType) {
        this.source = source;
        this.mapName = mapName;
        this.caller = caller;
        this.eventType = eventType;
    }

    @Override
    public String getSource() {
        return source;
    }

    @Override
    public String getMapName() {
        return mapName;
    }

    @Override
    public Address getCaller() {
        return caller;
    }

    @Override
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
