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

package com.hazelcast.replicatedmap.operation;

import com.hazelcast.monitor.impl.LocalReplicatedMapStatsImpl;
import com.hazelcast.nio.serialization.ArrayDataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.messages.MultiReplicationMessage;
import com.hazelcast.replicatedmap.messages.ReplicationMessage;
import com.hazelcast.replicatedmap.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.record.VectorClock;
import com.hazelcast.util.ConstructorFunction;

/**
 * This class contains all the ID hooks for IdentifiedDataSerializable classes used inside the replicated map.
 */
//Deactivated all checkstyle rules because those classes will never comply
//CHECKSTYLE:OFF
public class ReplicatedMapDataSerializerHook
        implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.REPLICATED_MAP_DS_FACTORY, -22);

    public static final int VECTOR = 0;
    public static final int RECORD = 1;
    public static final int REPL_UPDATE_MESSAGE = 2;
    public static final int REPL_CLEAR_MESSAGE = 3;
    public static final int REPL_MULTI_UPDATE_MESSAGE = 4;
    public static final int OP_INIT_CHUNK = 5;
    public static final int OP_POST_JOIN = 6;
    public static final int OP_CLEAR = 7;
    public static final int MAP_STATS = 8;

    private static final int LEN = MAP_STATS + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];
        constructors[VECTOR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new VectorClock();
            }
        };
        constructors[RECORD] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReplicatedRecord();
            }
        };
        constructors[REPL_UPDATE_MESSAGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReplicationMessage();
            }
        };
        constructors[REPL_CLEAR_MESSAGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new VectorClock();
            }
        };
        constructors[REPL_MULTI_UPDATE_MESSAGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MultiReplicationMessage();
            }
        };
        constructors[OP_INIT_CHUNK] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReplicatedMapInitChunkOperation();
            }
        };
        constructors[OP_POST_JOIN] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReplicatedMapPostJoinOperation();
            }
        };
        constructors[OP_CLEAR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReplicatedMapClearOperation();
            }
        };
        constructors[MAP_STATS] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LocalReplicatedMapStatsImpl();
            }
        };

        return new ArrayDataSerializableFactory(constructors);
    }
}
