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

package com.hazelcast.client.executor.tasks;

import com.hazelcast.core.*;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;


/**
 * this task should execute on a node owning the given partitionKey argument,
 * the action is to put the UUid of the executing node into a map with the given name
 */
public class MapPutPartitionAwareRunnable<P> implements Runnable, DataSerializable, PartitionAware<P>, HazelcastInstanceAware {

    private HazelcastInstance instance;

    public String mapName;
    public P partitionKey;

    @SuppressWarnings("unused")
    public MapPutPartitionAwareRunnable(){}

    public MapPutPartitionAwareRunnable(String mapName, P partitionKey) {
        this.mapName = mapName;
        this.partitionKey = partitionKey;
    }

    @Override
    public void run() {
        final Member member = instance.getCluster().getLocalMember();

        final IMap<String, String> map = instance.getMap(mapName);
        map.put(member.getUuid(), member.getUuid()+"value");
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
    }

    @Override
    public P getPartitionKey() {
        return partitionKey;
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
