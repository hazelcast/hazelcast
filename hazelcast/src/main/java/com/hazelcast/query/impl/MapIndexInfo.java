/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class MapIndexInfo implements IdentifiedDataSerializable {

    private String mapName;
    private List<IndexInfo> lsIndexes = new LinkedList<IndexInfo>();

    public MapIndexInfo(String mapName) {
        this.mapName = mapName;
    }

    public MapIndexInfo() {
    }

    public void addIndexInfo(String attributeName, boolean ordered) {
        lsIndexes.add(new IndexInfo(attributeName, ordered));
    }

    public void addIndexInfos(Collection<IndexInfo> indexInfos) {
        lsIndexes.addAll(indexInfos);
    }

    public String getMapName() {
        return mapName;
    }

    public List<IndexInfo> getLsIndexes() {
        return lsIndexes;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeInt(lsIndexes.size());
        for (IndexInfo indexInfo : lsIndexes) {
            indexInfo.writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            IndexInfo indexInfo = new IndexInfo();
            indexInfo.readData(in);
            lsIndexes.add(indexInfo);
        }
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.MAP_INDEX_INFO;
    }

}
