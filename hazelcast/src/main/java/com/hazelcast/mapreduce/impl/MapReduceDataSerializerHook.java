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

import com.hazelcast.mapreduce.impl.notification.IntermediateChunkNotification;
import com.hazelcast.mapreduce.impl.notification.LastChunkNotification;
import com.hazelcast.mapreduce.impl.notification.ReducingFinishedNotification;
import com.hazelcast.mapreduce.impl.operation.CancelJobSupervisorOperation;
import com.hazelcast.mapreduce.impl.operation.FireNotificationOperation;
import com.hazelcast.mapreduce.impl.operation.GetResultOperation;
import com.hazelcast.mapreduce.impl.operation.KeyValueJobOperation;
import com.hazelcast.mapreduce.impl.operation.KeysAssignmentOperation;
import com.hazelcast.mapreduce.impl.operation.KeysAssignmentResult;
import com.hazelcast.mapreduce.impl.operation.NotifyRemoteExceptionOperation;
import com.hazelcast.mapreduce.impl.operation.PostPonePartitionProcessing;
import com.hazelcast.mapreduce.impl.operation.ProcessStatsUpdateOperation;
import com.hazelcast.mapreduce.impl.operation.RequestMemberIdAssignment;
import com.hazelcast.mapreduce.impl.operation.RequestPartitionMapping;
import com.hazelcast.mapreduce.impl.operation.RequestPartitionProcessed;
import com.hazelcast.mapreduce.impl.operation.RequestPartitionReducing;
import com.hazelcast.mapreduce.impl.operation.RequestPartitionResult;
import com.hazelcast.mapreduce.impl.operation.StartProcessingJobOperation;
import com.hazelcast.nio.serialization.ArrayDataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;

/**
 * This class contains all the ID hooks for IdentifiedDataSerializable classes used inside the MR framework.
 */
//Deactivated all checkstyle rules because those classes will never comply
//CHECKSTYLE:OFF
public class MapReduceDataSerializerHook
        implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.MAP_REDUCE_DS_FACTORY, -23);

    public static final int KEY_VALUE_SOURCE_MAP = 0;
    public static final int KEY_VALUE_SOURCE_MULTIMAP = 1;
    public static final int REDUCER_CHUNK_MESSAGE = 2;
    public static final int REDUCER_LAST_CHUNK_MESSAGE = 3;
    public static final int TRACKED_JOB_OPERATION = 4;
    public static final int REQUEST_PARTITION_MAPPING = 5;
    public static final int REQUEST_PARTITION_REDUCING = 6;
    public static final int REQUEST_PARTITION_PROCESSED = 7;
    public static final int GET_RESULT_OPERATION = 8;
    public static final int START_PROCESSING_OPERATION = 9;
    public static final int REQUEST_PARTITION_RESULT = 10;
    public static final int REDUCING_FINISHED_MESSAGE = 11;
    public static final int FIRE_NOTIFICATION_OPERATION = 12;
    public static final int REQUEST_MEMBERID_ASSIGNMENT = 13;
    public static final int PROCESS_STATS_UPDATE_OPERATION = 14;
    public static final int NOTIFY_REMOTE_EXCEPTION_OPERATION = 15;
    public static final int CANCEL_JOB_SUPERVISOR_OPERATION = 16;
    public static final int POSTPONE_PARTITION_PROCESSING_OPERATION = 17;
    public static final int KEY_VALUE_SOURCE_LIST = 18;
    public static final int KEY_VALUE_SOURCE_SET = 19;
    public static final int KEYS_ASSIGNMENT_RESULT = 20;
    public static final int KEYS_ASSIGNMENT_OPERATION = 21;
    public static final int HASH_MAP_ADAPTER = 22;

    private static final int LEN = HASH_MAP_ADAPTER + 1;

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
        constructors[REQUEST_PARTITION_MAPPING] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RequestPartitionMapping();
            }
        };
        constructors[REQUEST_PARTITION_REDUCING] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RequestPartitionReducing();
            }
        };
        constructors[REQUEST_PARTITION_PROCESSED] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RequestPartitionProcessed();
            }
        };
        constructors[GET_RESULT_OPERATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new GetResultOperation();
            }
        };
        constructors[START_PROCESSING_OPERATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new StartProcessingJobOperation();
            }
        };
        constructors[REQUEST_PARTITION_RESULT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RequestPartitionResult();
            }
        };
        constructors[REDUCING_FINISHED_MESSAGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReducingFinishedNotification();
            }
        };
        constructors[FIRE_NOTIFICATION_OPERATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new FireNotificationOperation();
            }
        };
        constructors[REQUEST_MEMBERID_ASSIGNMENT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RequestMemberIdAssignment();
            }
        };
        constructors[PROCESS_STATS_UPDATE_OPERATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ProcessStatsUpdateOperation();
            }
        };
        constructors[NOTIFY_REMOTE_EXCEPTION_OPERATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new NotifyRemoteExceptionOperation();
            }
        };
        constructors[CANCEL_JOB_SUPERVISOR_OPERATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CancelJobSupervisorOperation();
            }
        };
        constructors[KEY_VALUE_SOURCE_LIST] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ListKeyValueSource();
            }
        };
        constructors[KEY_VALUE_SOURCE_SET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SetKeyValueSource();
            }
        };
        constructors[KEYS_ASSIGNMENT_RESULT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new KeysAssignmentResult();
            }
        };
        constructors[KEYS_ASSIGNMENT_OPERATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new KeysAssignmentOperation();
            }
        };
        constructors[POSTPONE_PARTITION_PROCESSING_OPERATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PostPonePartitionProcessing();
            }
        };
        constructors[HASH_MAP_ADAPTER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HashMapAdapter();
            }
        };
        return new ArrayDataSerializableFactory(constructors);
    }

}
