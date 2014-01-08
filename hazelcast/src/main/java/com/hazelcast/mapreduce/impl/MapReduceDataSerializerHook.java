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

package com.hazelcast.mapreduce.impl;

import com.hazelcast.mapreduce.impl.client.ClientMapReduceRequest;
import com.hazelcast.mapreduce.impl.notification.IntermediateChunkNotification;
import com.hazelcast.mapreduce.impl.notification.LastChunkNotification;
import com.hazelcast.mapreduce.impl.operation.KeyValueJobOperation;
import com.hazelcast.mapreduce.impl.operation.RequestPartitionProcessed;
import com.hazelcast.mapreduce.impl.operation.RequestPartitionProcessing;
import com.hazelcast.mapreduce.impl.task.JobPartitionStateImpl;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.util.ConstructorFunction;

public class MapReduceDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.MAP_REDUCE_DS_FACTORY, -23);

    public static final int KEY_VALUE_SOURCE_MAP = 0;
    public static final int KEY_VALUE_SOURCE_MULTIMAP = 1;
    public static final int REDUCER_CHUNK_MESSAGE = 2;
    public static final int REDUCER_LAST_CHUNK_MESSAGE = 3;
    public static final int TRACKED_JOB_OPERATION = 4;
    public static final int REQUEST_PARTITION_PROCESSING = 5;
    public static final int REQUEST_PARTITION_PROCESSED = 6;
    public static final int PARTITION_STATE = 7;
    public static final int CLIENT_MAP_REDUCE_REQUEST = 8;

    public static final int LEN = CLIENT_MAP_REDUCE_REQUEST + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable> constructors[] = new ConstructorFunction[LEN];
        constructors[KEY_VALUE_SOURCE_MAP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapKeyValueSource();
            }
        };
        constructors[KEY_VALUE_SOURCE_MULTIMAP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MultiMapKeyValueSource();
            }
        };
        constructors[REDUCER_CHUNK_MESSAGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new IntermediateChunkNotification();
            }
        };
        constructors[REDUCER_LAST_CHUNK_MESSAGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LastChunkNotification();
            }
        };
        constructors[TRACKED_JOB_OPERATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new KeyValueJobOperation();
            }
        };
        constructors[REQUEST_PARTITION_PROCESSING] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RequestPartitionProcessing();
            }
        };
        constructors[REQUEST_PARTITION_PROCESSED] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RequestPartitionProcessed();
            }
        };
        constructors[PARTITION_STATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new JobPartitionStateImpl();
            }
        };
        constructors[CLIENT_MAP_REDUCE_REQUEST] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClientMapReduceRequest();
            }
        };
        return new ArrayDataSerializableFactory(constructors);
    }

}
