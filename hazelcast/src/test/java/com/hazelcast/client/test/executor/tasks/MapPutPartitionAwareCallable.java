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

package com.hazelcast.client.test.executor.tasks;

import com.hazelcast.client.test.IdentifiedFactory;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * This class is for Non-java clients as well. Please do not remove or modify.
 * <p>
 * this task should execute on a node owning the given partitionKey argument,
 * the action is to put the UUid of the executing node into a map with the given name
 * and return that UUid
 */
public class MapPutPartitionAwareCallable<T, P>
        implements Callable<T>, IdentifiedDataSerializable, PartitionAware<P>, HazelcastInstanceAware {
    public static final int CLASS_ID = 9;

    private HazelcastInstance instance;

    public String mapName;
    private P partitionKey;

    public MapPutPartitionAwareCallable() {
    }

    public MapPutPartitionAwareCallable(String mapName, P partitionKey) {
        this.mapName = mapName;
        this.partitionKey = partitionKey;
    }

    @Override
    public T call()
            throws Exception {
        Member member = instance.getCluster().getLocalMember();

        IMap<UUID, String> map = instance.getMap(mapName);
        map.put(member.getUuid(), member.getUuid() + "value");

        return (T) member.getUuid();
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeString(mapName);
        out.writeObject(partitionKey);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        mapName = in.readString();
        partitionKey = in.readObject();
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

    @Override
    public int getFactoryId() {
        return IdentifiedFactory.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return CLASS_ID;
    }
}
