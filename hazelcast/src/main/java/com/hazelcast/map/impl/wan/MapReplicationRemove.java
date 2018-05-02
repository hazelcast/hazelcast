/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.wan;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.impl.WanDataSerializerHook;
import com.hazelcast.wan.impl.WanEventCounter;

import java.io.IOException;

public class MapReplicationRemove implements ReplicationEventObject, IdentifiedDataSerializable {
    private String mapName;
    private Data key;
    private long removeTime;

    public MapReplicationRemove(String mapName, Data key, long removeTime) {
        this.mapName = mapName;
        this.key = key;
        this.removeTime = removeTime;
    }

    public MapReplicationRemove() {
    }

    public String getMapName() {
        return mapName;
    }

    public void setMapName(String mapName) {
        this.mapName = mapName;
    }

    @Override
    public Data getKey() {
        return key;
    }

    public void setKey(Data key) {
        this.key = key;
    }

    public long getRemoveTime() {
        return removeTime;
    }

    public void setRemoveTime(long removeTime) {
        this.removeTime = removeTime;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeLong(removeTime);
        out.writeData(key);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        removeTime = in.readLong();
        key = in.readData();
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return WanDataSerializerHook.MAP_REPLICATION_REMOVE;
    }

    @Override
    public void incrementEventCount(WanEventCounter eventCounter) {
        eventCounter.incrementRemove(mapName);
    }
}
