package com.hazelcast.map;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Map wide event's data.
 */
public class MapWideEventData extends AbstractEventData {

    protected int numberOfEntries;

    public MapWideEventData() {
    }

    public MapWideEventData(String source, String mapName, Address caller, int eventType, int numberOfEntries) {
        super(source, mapName, caller, eventType);
        this.numberOfEntries = numberOfEntries;
    }

    public int getNumberOfEntries() {
        return numberOfEntries;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(numberOfEntries);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        numberOfEntries = in.readInt();
    }

    @Override
    public String toString() {
        return "MapWideEventData{"
                + super.toString()
                + ", numberOfEntries=" + numberOfEntries
                + '}';
    }
}
