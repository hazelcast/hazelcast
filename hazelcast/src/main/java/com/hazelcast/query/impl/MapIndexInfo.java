/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.IndexConfig;
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
    private List<IndexConfig> indexConfigs = new LinkedList<>();

    public MapIndexInfo(String mapName) {
        this.mapName = mapName;
    }

    public MapIndexInfo() {
    }

    public MapIndexInfo addIndexCofigs(Collection<IndexConfig> indexConfigs) {
        this.indexConfigs.addAll(indexConfigs);
        return this;
    }

    public String getMapName() {
        return mapName;
    }

    public List<IndexConfig> getIndexConfigs() {
        return indexConfigs;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(mapName);
        out.writeInt(indexConfigs.size());

        for (IndexConfig indexConfig : indexConfigs) {
            out.writeObject(indexConfig);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readString();
        int size = in.readInt();

        for (int i = 0; i < size; i++) {
            IndexConfig indexConfig = in.readObject();

            indexConfigs.add(indexConfig);
        }
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.MAP_INDEX_INFO;
    }

}
