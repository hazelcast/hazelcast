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

package com.hazelcast.replicatedmap.impl.operation;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.impl.record.RecordMigrationInfo;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryView;
import com.hazelcast.internal.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.REPLICATED_MAP_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.REPLICATED_MAP_DS_FACTORY_ID;

/**
 * This class contains all the ID hooks for IdentifiedDataSerializable classes used inside the replicated map.
 */
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
    public static final int VERSION_RESPONSE_PAIR = 9;
    public static final int GET = 10;
    public static final int CHECK_REPLICA_VERSION = 11;
    public static final int CONTAINS_KEY = 12;
    public static final int CONTAINS_VALUE = 13;
    public static final int ENTRY_SET = 14;
    public static final int EVICTION = 15;
    public static final int IS_EMPTY = 16;
    public static final int KEY_SET = 17;
    public static final int REPLICATION = 18;
    public static final int REQUEST_MAP_DATA = 19;
    public static final int SYNC_REPLICATED_DATA = 20;
    public static final int VALUES = 21;
    public static final int CLEAR_OP_FACTORY = 22;
    public static final int PUT_ALL_OP_FACTORY = 23;
    public static final int RECORD_MIGRATION_INFO = 24;
    public static final int MERGE_FACTORY = 25;
    public static final int MERGE = 26;

    private static final int LEN = MERGE + 1;

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
        constructors[CLEAR] = arg -> new ClearOperation();
        constructors[ENTRY_VIEW] = arg -> new ReplicatedMapEntryView();
        constructors[REPLICATE_UPDATE] = arg -> new ReplicateUpdateOperation();
        constructors[REPLICATE_UPDATE_TO_CALLER] = arg -> new ReplicateUpdateToCallerOperation();
        constructors[PUT_ALL] = arg -> new PutAllOperation();
        constructors[PUT] = arg -> new PutOperation();
        constructors[REMOVE] = arg -> new RemoveOperation();
        constructors[SIZE] = arg -> new SizeOperation();
        constructors[VERSION_RESPONSE_PAIR] = arg -> new VersionResponsePair();
        constructors[GET] = arg -> new GetOperation();
        constructors[CHECK_REPLICA_VERSION] = arg -> new CheckReplicaVersionOperation();
        constructors[CONTAINS_KEY] = arg -> new ContainsKeyOperation();
        constructors[CONTAINS_VALUE] = arg -> new ContainsValueOperation();
        constructors[ENTRY_SET] = arg -> new EntrySetOperation();
        constructors[EVICTION] = arg -> new EvictionOperation();
        constructors[IS_EMPTY] = arg -> new IsEmptyOperation();
        constructors[KEY_SET] = arg -> new KeySetOperation();
        constructors[REPLICATION] = arg -> new ReplicationOperation();
        constructors[REQUEST_MAP_DATA] = arg -> new RequestMapDataOperation();
        constructors[SYNC_REPLICATED_DATA] = arg -> new SyncReplicatedMapDataOperation();
        constructors[VALUES] = arg -> new ValuesOperation();
        constructors[CLEAR_OP_FACTORY] = arg -> new ClearOperationFactory();
        constructors[PUT_ALL_OP_FACTORY] = arg -> new PutAllOperationFactory();
        constructors[RECORD_MIGRATION_INFO] = arg -> new RecordMigrationInfo();
        constructors[MERGE_FACTORY] = arg -> new MergeOperationFactory();
        constructors[MERGE] = arg -> new MergeOperation();

        return new ArrayDataSerializableFactory(constructors);
    }
}
