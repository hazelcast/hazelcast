/**
 * 
 */
package com.hazelcast.impl.concurrentmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.hazelcast.cluster.AbstractRemotelyProcessable;
import com.hazelcast.impl.CMap;
import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.query.Index;

public class InitialState extends AbstractRemotelyProcessable {
    List<MapState> lsMapStates = new ArrayList();

    public InitialState() {
    }

    public void createAndAddMapState(CMap cmap) {
        MapState mapState = new MapState(cmap.getName());
        int indexCount = cmap.getMapIndexes().size();
        for (int i = 0; i < indexCount; i++) {
            Index index = cmap.getIndexes()[i];
            AddMapIndex mi = new AddMapIndex(cmap.getName(), index.getExpression(), index.isOrdered());
            mapState.addMapIndex(mi);
        }
        lsMapStates.add(mapState);
    }

    public void process() {
        FactoryImpl factory = getNode().factory;
        if (factory.node.isActive()) {
            for (MapState mapState : lsMapStates) {
                CMap cmap = factory.node.concurrentMapManager.getOrCreateMap(mapState.name);
                for (AddMapIndex mapIndex : mapState.lsMapIndexes) {
                    cmap.addIndex(mapIndex.getExpression(), mapIndex.isOrdered());
                }
            }
        }
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeInt(lsMapStates.size());
        for (MapState mapState : lsMapStates) {
            mapState.writeData(out);
        }
    }

    public void readData(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            MapState mapState = new MapState();
            mapState.readData(in);
            lsMapStates.add(mapState);
        }
    }
}