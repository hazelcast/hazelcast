/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.EntryView;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.impl.DistributedServiceWanEventCounters;
import com.hazelcast.wan.impl.WanDataSerializerHook;

import java.io.IOException;

/**
 * WAN replication object for map update operations.
 */
public class MapReplicationUpdate implements ReplicationEventObject, IdentifiedDataSerializable {
    private String mapName;
    /** The policy how to merge the entry on the receiving cluster */
    private Object mergePolicy;
    /** The updated entry */
    private WanMapEntryView<Data, Data> entryView;

    public MapReplicationUpdate() {
    }

    public MapReplicationUpdate(String mapName,
                                Object mergePolicy,
                                EntryView<Data, Data> entryView) {
        this.mergePolicy = mergePolicy;
        this.mapName = mapName;
        if (entryView instanceof WanMapEntryView) {
            this.entryView = (WanMapEntryView<Data, Data>) entryView;
        } else {
            this.entryView = new WanMapEntryView<Data, Data>(entryView);
        }
    }

    public String getMapName() {
        return mapName;
    }

    public void setMapName(String mapName) {
        this.mapName = mapName;
    }

    public Object getMergePolicy() {
        return mergePolicy;
    }

    public void setMergePolicy(Object mergePolicy) {
        this.mergePolicy = mergePolicy;
    }

    public WanMapEntryView<Data, Data> getEntryView() {
        return entryView;
    }

    public void setEntryView(WanMapEntryView<Data, Data> entryView) {
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
        EntryView<Data, Data> entryView = in.readObject();

        if (entryView instanceof WanMapEntryView) {
            this.entryView = (WanMapEntryView<Data, Data>) entryView;
        } else {
            this.entryView = new WanMapEntryView<Data, Data>(entryView);
        }
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return WanDataSerializerHook.MAP_REPLICATION_UPDATE;
    }

    @Override
    public void incrementEventCount(DistributedServiceWanEventCounters counters) {
        counters.incrementUpdate(mapName);
    }

    @Override
    public Data getKey() {
        return entryView.getKey();
    }
}
