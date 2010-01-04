/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl.concurrentmap;

import com.hazelcast.cluster.AbstractRemotelyProcessable;
import com.hazelcast.impl.CMap;
import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.query.Index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
