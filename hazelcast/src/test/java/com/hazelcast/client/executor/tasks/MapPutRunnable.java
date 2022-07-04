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

package com.hazelcast.client.executor.tasks;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.IMap;
import com.hazelcast.cluster.Member;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.UUID;

public class MapPutRunnable implements Runnable, DataSerializable, HazelcastInstanceAware {

    private HazelcastInstance instance;

    private String mapName;

    @SuppressWarnings("unused")
    public MapPutRunnable() {
    }

    public MapPutRunnable(String mapName) {
        this.mapName = mapName;
    }

    @Override
    public void run() {
        Member member = instance.getCluster().getLocalMember();

        IMap<UUID, String> map = instance.getMap(mapName);
        map.put(member.getUuid(), member.getUuid() + "value");
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(mapName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readString();
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        instance = hazelcastInstance;
    }

    public String getMapName() {
        return mapName;
    }

    public void setMapName(String mapName) {
        this.mapName = mapName;
    }
}
