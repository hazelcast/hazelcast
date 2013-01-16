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

package com.hazelcast.impl.concurrentmap;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.impl.*;
import com.hazelcast.impl.base.Pairs;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.query.Predicate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.Callable;

import static com.hazelcast.nio.IOUtil.toObject;

public class MapQueryCallable implements Callable<Pairs>, DataSerializable, HazelcastInstanceAware {
    private Data predicateData;
    private int partitionVersion;
    private String mapName;
    private ClusterOperation operation;
    private transient HazelcastInstance hazelcast;

    public MapQueryCallable(String mapName, ClusterOperation operation, Data predicateData, int partitionVersion) {
        this.mapName = mapName;
        this.operation = operation;
        this.predicateData = predicateData;
        this.partitionVersion = partitionVersion;
    }

    public MapQueryCallable() {
    }

    public Pairs call() throws Exception {
        FactoryImpl factory = (FactoryImpl) hazelcast;
        ConcurrentMapManager concurrentMapManager = factory.node.concurrentMapManager;
        CMap cmap = concurrentMapManager.getMap(mapName);
        if (cmap == null) return new Pairs();
        PartitionManager partitionManager = concurrentMapManager.getPartitionManager();
        if (partitionManager.getVersion() != partitionVersion) return null;
        Pairs pairs = factory.node.concurrentMapManager.queryMap(cmap, operation, (Predicate) toObject(predicateData));
        if (partitionManager.getVersion() != partitionVersion) return null;
        return pairs;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeShort(operation.getValue());
        out.writeInt(partitionVersion);
        boolean hasPredicate = predicateData != null;
        out.writeBoolean(hasPredicate);
        if (hasPredicate) {
            predicateData.writeData(out);
        }
    }

    public void readData(DataInput in) throws IOException {
        mapName = in.readUTF();
        operation = ClusterOperation.create(in.readShort());
        partitionVersion = in.readInt();
        boolean hasPredicate = in.readBoolean();
        if (hasPredicate) {
            predicateData = new Data();
            predicateData.readData(in);
        }
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcast = hazelcastInstance;
    }
}
