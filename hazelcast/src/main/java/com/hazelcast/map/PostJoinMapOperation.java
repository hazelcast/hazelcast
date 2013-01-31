/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.cluster.JoinOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class PostJoinMapOperation extends AbstractOperation implements JoinOperation {
    private List<MapIndexInfo> lsMapIndexes = new LinkedList<MapIndexInfo>();

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    void addMapIndex(MapContainer mapContainer) {
        final IndexService indexService = mapContainer.getIndexService();
        if (indexService.hasIndex()) {
            MapIndexInfo mapIndexInfo = new MapIndexInfo(mapContainer.getName());
            for (Index index : indexService.getIndexes()) {
                mapIndexInfo.addIndexInfo(index.getAttributeName(), index.isOrdered());
            }
            lsMapIndexes.add(mapIndexInfo);
        }
    }

    class MapIndexInfo implements DataSerializable {
        String mapName;
        private List<MapIndexInfo.IndexInfo> lsIndexes = new LinkedList<MapIndexInfo.IndexInfo>();

        public MapIndexInfo(String mapName) {
            this.mapName = mapName;
        }

        public MapIndexInfo() {
        }

        class IndexInfo implements DataSerializable {
            String attributeName;
            boolean ordered;

            IndexInfo() {
            }

            IndexInfo(String attributeName, boolean ordered) {
                this.attributeName = attributeName;
                this.ordered = ordered;
            }

            public void writeData(ObjectDataOutput out) throws IOException {
                out.writeUTF(attributeName);
                out.writeBoolean(ordered);
            }

            public void readData(ObjectDataInput in) throws IOException {
                attributeName = in.readUTF();
                ordered = in.readBoolean();
            }
        }

        public void addIndexInfo(String attributeName, boolean ordered) {
            lsIndexes.add(new MapIndexInfo.IndexInfo(attributeName, ordered));
        }

        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(mapName);
            out.writeInt(lsIndexes.size());
            for (MapIndexInfo.IndexInfo indexInfo : lsIndexes) {
                indexInfo.writeData(out);
            }
        }

        public void readData(ObjectDataInput in) throws IOException {
            mapName = in.readUTF();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                MapIndexInfo.IndexInfo indexInfo = new MapIndexInfo.IndexInfo();
                indexInfo.readData(in);
                lsIndexes.add(indexInfo);
            }
        }
    }

    @Override
    public void run() throws Exception {
        MapService mapService = getService();
        for (MapIndexInfo mapIndex : lsMapIndexes) {
            final MapContainer mapContainer = mapService.getMapContainer(mapIndex.mapName);
            final IndexService indexService = mapContainer.getIndexService();
            for (MapIndexInfo.IndexInfo indexInfo : mapIndex.lsIndexes) {
                indexService.addOrGetIndex(indexInfo.attributeName, indexInfo.ordered);
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(lsMapIndexes.size());
        for (MapIndexInfo mapIndex : lsMapIndexes) {
            mapIndex.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            MapIndexInfo mapIndexInfo = new MapIndexInfo();
            mapIndexInfo.readData(in);
            lsMapIndexes.add(mapIndexInfo);
        }
    }
}
