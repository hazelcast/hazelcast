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

package com.hazelcast.map.wan;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.wan.ReplicationEventObject;
import java.io.IOException;

public class MapReplicationUpdate implements ReplicationEventObject, DataSerializable {

    String mapName;
    MapMergePolicy mergePolicy;
    EntryView entryView;

    public MapReplicationUpdate() {
    }

    public MapReplicationUpdate(String mapName, MapMergePolicy mergePolicy, EntryView entryView) {
        this.mergePolicy = mergePolicy;
        this.mapName = mapName;
        this.entryView = entryView;
    }

    public String getMapName() {
        return mapName;
    }

    public void setMapName(String mapName) {
        this.mapName = mapName;
    }

    public MapMergePolicy getMergePolicy() {
        return mergePolicy;
    }

    public void setMergePolicy(MapMergePolicy mergePolicy) {
        this.mergePolicy = mergePolicy;
    }

    public EntryView getEntryView() {
        return entryView;
    }

    public void setEntryView(EntryView entryView) {
        this.entryView = entryView;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeObject(mergePolicy);
        out.writeObject(entryView);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        mergePolicy = in.readObject();
        entryView = in.readObject();
    }
}
