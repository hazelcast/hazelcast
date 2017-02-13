/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.operation;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.impl.record.RecordMigrationInfo;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryView;
import com.hazelcast.replicatedmap.merge.HigherHitsMapMergePolicy;
import com.hazelcast.replicatedmap.merge.LatestUpdateMapMergePolicy;
import com.hazelcast.replicatedmap.merge.PassThroughMergePolicy;
import com.hazelcast.replicatedmap.merge.PutIfAbsentMapMergePolicy;
import com.hazelcast.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.REPLICATED_MAP_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.REPLICATED_MAP_DS_FACTORY_ID;

/**
 * This class contains all the ID hooks for IdentifiedDataSerializable classes used inside the replicated map.
 */
//Deactivated all checkstyle rules because those classes will never comply
//CHECKSTYLE:OFF
public class ReplicatedMapDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(REPLICATED_MAP_DS_FACTORY, REPLICATED_MAP_DS_FACTORY_ID);

    public static final int CLEAR = 1;
    public static final int ENTRY_VIEW = 2;
    public static final int REPLICATE_UPDATE = 3;
    public static final int REPLICATE_UPDATE_TO_CALLER = 4;
    public static final int PUT_ALL = 5;
    public static final int PUT = 6;
    public static final int REMOVE = 7;
    public static final int SIZE = 8;
    public static final int MERGE = 9;
    public static final int VERSION_RESPONSE_PAIR = 10;
    public static final int GET = 11;
    public static final int CHECK_REPLICA_VERSION = 12;
    public static final int CONTAINS_KEY = 13;
    public static final int CONTAINS_VALUE = 14;
    public static final int ENTRY_SET = 15;
    public static final int EVICTION = 16;
    public static final int IS_EMPTY = 17;
    public static final int KEY_SET = 18;
    public static final int REPLICATION = 19;
    public static final int REQUEST_MAP_DATA = 20;
    public static final int SYNC_REPLICATED_DATA = 21;
    public static final int VALUES = 22;
    public static final int CLEAR_OP_FACTORY = 23;
    public static final int PUT_ALL_OP_FACTORY = 24;
    public static final int RECORD_MIGRATION_INFO = 25;
    public static final int HIGHER_HITS_MERGE_POLICY = 26;
    public static final int LATEST_UPDATE_MERGE_POLICY = 27;
    public static final int PASS_THROUGH_MERGE_POLICY = 28;
    public static final int PUT_IF_ABSENT_MERGE_POLICY = 29;

    private static final int LEN = PUT_IF_ABSENT_MERGE_POLICY + 1;

    private static final DataSerializableFactory FACTORY = createFactoryInternal();

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return FACTORY;
    }

    private static DataSerializableFactory createFactoryInternal() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];
        constructors[CLEAR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClearOperation();
            }
        };
        constructors[ENTRY_VIEW] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReplicatedMapEntryView();
            }
        };
        constructors[REPLICATE_UPDATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReplicateUpdateOperation();
            }
        };
        constructors[REPLICATE_UPDATE_TO_CALLER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReplicateUpdateToCallerOperation();
            }
        };
        constructors[PUT_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PutAllOperation();
            }
        };
        constructors[PUT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PutOperation();
            }
        };
        constructors[REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RemoveOperation();
            }
        };
        constructors[SIZE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SizeOperation();
            }
        };
        constructors[MERGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MergeOperation();
            }
        };
        constructors[VERSION_RESPONSE_PAIR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new VersionResponsePair();
            }
        };
        constructors[GET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new GetOperation();
            }
        };
        constructors[CHECK_REPLICA_VERSION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CheckReplicaVersionOperation();
            }
        };
        constructors[CONTAINS_KEY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ContainsKeyOperation();
            }
        };
        constructors[CONTAINS_VALUE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ContainsValueOperation();
            }
        };
        constructors[ENTRY_SET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new EntrySetOperation();
            }
        };
        constructors[EVICTION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new EvictionOperation();
            }
        };
        constructors[IS_EMPTY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new IsEmptyOperation();
            }
        };
        constructors[KEY_SET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new KeySetOperation();
            }
        };
        constructors[REPLICATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReplicationOperation();
            }
        };
        constructors[REQUEST_MAP_DATA] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RequestMapDataOperation();
            }
        };
        constructors[SYNC_REPLICATED_DATA] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SyncReplicatedMapDataOperation();
            }
        };
        constructors[VALUES] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ValuesOperation();
            }
        };
        constructors[CLEAR_OP_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClearOperationFactory();
            }
        };
        constructors[PUT_ALL_OP_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PutAllOperationFactory();
            }
        };
        constructors[RECORD_MIGRATION_INFO] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RecordMigrationInfo();
            }
        };
        constructors[HIGHER_HITS_MERGE_POLICY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return HigherHitsMapMergePolicy.INSTANCE;
            }
        };
        constructors[LATEST_UPDATE_MERGE_POLICY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return LatestUpdateMapMergePolicy.INSTANCE;
            }
        };
        constructors[PASS_THROUGH_MERGE_POLICY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return PassThroughMergePolicy.INSTANCE;
            }
        };
        constructors[PUT_IF_ABSENT_MERGE_POLICY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return PutIfAbsentMapMergePolicy.INSTANCE;
            }
        };

        return new ArrayDataSerializableFactory(constructors);
    }

}
