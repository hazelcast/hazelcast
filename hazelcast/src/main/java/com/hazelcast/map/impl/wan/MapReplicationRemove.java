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

import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.wan.DistributedServiceWanEventCounters;
import com.hazelcast.wan.impl.InternalWanReplicationEvent;
import com.hazelcast.wan.impl.WanDataSerializerHook;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

public class MapReplicationRemove implements InternalWanReplicationEvent, IdentifiedDataSerializable {
    private String mapName;
    private Data key;

    public MapReplicationRemove(String mapName, Data key) {
        this.mapName = mapName;
        this.key = key;
    }

    public MapReplicationRemove() {
    }

    @Nonnull
    @Override
    public Data getKey() {
        return key;
    }

    @Nonnull
    @Override
    public Set<String> getClusterNames() {
        // called only in EE
        return Collections.emptySet();
    }

    @Override
    public int getBackupCount() {
        // called only in EE
        return 0;
    }

    @Override
    public long getCreationTime() {
        // called only in EE
        return 0;
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public String getObjectName() {
        return mapName;
    }

    public void setKey(Data key) {
        this.key = key;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeData(key);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        key = in.readData();
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.MAP_REPLICATION_REMOVE;
    }

    @Override
    public void incrementEventCount(DistributedServiceWanEventCounters counters) {
        counters.incrementRemove(mapName);
    }
}
