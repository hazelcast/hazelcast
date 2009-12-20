/**
 * 
 */
package com.hazelcast.impl.concurrentmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.hazelcast.nio.DataSerializable;

class MapState implements DataSerializable {
    String name;
    List<AddMapIndex> lsMapIndexes = new ArrayList<AddMapIndex>();

    MapState() {
    }

    MapState(String name) {
        this.name = name;
    }

    void addMapIndex(AddMapIndex mapIndex) {
        lsMapIndexes.add(mapIndex);
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(lsMapIndexes.size());
        for (AddMapIndex mapIndex : lsMapIndexes) {
            mapIndex.writeData(out);
        }
    }

    public void readData(DataInput in) throws IOException {
        name = in.readUTF();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            AddMapIndex mapIndex = new AddMapIndex();
            mapIndex.readData(in);
            lsMapIndexes.add(mapIndex);
        }
    }
}