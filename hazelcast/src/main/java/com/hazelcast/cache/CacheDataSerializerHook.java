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

package com.hazelcast.cache;

import com.hazelcast.cache.operation.CacheClearBackupOperation;
import com.hazelcast.cache.operation.CacheClearOperation;
import com.hazelcast.cache.operation.CacheClearOperationFactory;
import com.hazelcast.cache.operation.CacheContainsKeyOperation;
import com.hazelcast.cache.operation.CacheCreateConfigOperation;
import com.hazelcast.cache.operation.CacheEntryProcessorOperation;
import com.hazelcast.cache.operation.CacheGetAllOperation;
import com.hazelcast.cache.operation.CacheGetAllOperationFactory;
import com.hazelcast.cache.operation.CacheGetAndRemoveOperation;
import com.hazelcast.cache.operation.CacheGetAndReplaceOperation;
import com.hazelcast.cache.operation.CacheGetConfigOperation;
import com.hazelcast.cache.operation.CacheGetOperation;
import com.hazelcast.cache.operation.CacheKeyIteratorOperation;
import com.hazelcast.cache.operation.CacheListenerRegistrationOperation;
import com.hazelcast.cache.operation.CacheLoadAllOperation;
import com.hazelcast.cache.operation.CacheLoadAllOperationFactory;
import com.hazelcast.cache.operation.CacheManagementConfigOperation;
import com.hazelcast.cache.operation.CachePutAllBackupOperation;
import com.hazelcast.cache.operation.CachePutBackupOperation;
import com.hazelcast.cache.operation.CachePutIfAbsentOperation;
import com.hazelcast.cache.operation.CachePutOperation;
import com.hazelcast.cache.operation.CacheRemoveBackupOperation;
import com.hazelcast.cache.operation.CacheRemoveOperation;
import com.hazelcast.cache.operation.CacheReplaceOperation;
import com.hazelcast.cache.operation.CacheSizeOperation;
import com.hazelcast.cache.operation.CacheSizeOperationFactory;
import com.hazelcast.nio.serialization.ArrayDataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;

public final class CacheDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.CACHE_DS_FACTORY, -25);
    private static short i = 0;
    public static final short GET = i++;
    public static final short CONTAINS_KEY = i++;
    public static final short PUT = i++;
    public static final short PUT_IF_ABSENT = i++;

    public static final short REMOVE = i++;

    public static final short GET_AND_REMOVE = i++;
    public static final short REPLACE = i++;
    public static final short GET_AND_REPLACE = i++;
    public static final short PUT_BACKUP = i++;
    public static final short PUT_ALL_BACKUP = i++;
    public static final short REMOVE_BACKUP = i++;

    public static final short CLEAR_BACKUP = i++;
    public static final short SIZE = i++;
    public static final short SIZE_FACTORY = i++;
    public static final short CLEAR = i++;

    public static final short CLEAR_FACTORY = i++;
    public static final short GET_ALL = i++;
    public static final short GET_ALL_FACTORY = i++;
    public static final short LOAD_ALL = i++;
    public static final short LOAD_ALL_FACTORY = i++;
    public static final short EXPIRY_POLICY = i++;
    public static final short KEY_ITERATOR = i++;

    public static final short KEY_ITERATION_RESULT = i++;
    public static final short ENTRY_PROCESSOR = i++;
    public static final short CLEAR_RESPONSE = i++;
    public static final short CREATE_CONFIG = i++;
    public static final short GET_CONFIG = i++;
    public static final short MANAGEMENT_CONFIG = i++;
    //    public static final short EVENT = i++;
    public static final short LISTENER_REGISTRATION = i++;

    private static final int LEN = i++;

    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];
        constructors[GET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheGetOperation();
            }
        };
        constructors[CONTAINS_KEY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheContainsKeyOperation();
            }
        };
        constructors[PUT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CachePutOperation();
            }
        };
        constructors[PUT_IF_ABSENT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CachePutIfAbsentOperation();
            }
        };
        constructors[REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheRemoveOperation();
            }
        };
        constructors[GET_AND_REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheGetAndRemoveOperation();
            }
        };
        constructors[REPLACE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheReplaceOperation();
            }
        };
        constructors[GET_AND_REPLACE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheGetAndReplaceOperation();
            }
        };
        constructors[PUT_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CachePutBackupOperation();
            }
        };
        constructors[PUT_ALL_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CachePutAllBackupOperation();
            }
        };
        constructors[REMOVE_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheRemoveBackupOperation();
            }
        };
        constructors[CLEAR_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheClearBackupOperation();
            }
        };
        constructors[SIZE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheSizeOperation();
            }
        };
        constructors[SIZE_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheSizeOperationFactory();
            }
        };
        constructors[CLEAR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheClearOperation();
            }
        };
        constructors[CLEAR_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheClearOperationFactory();
            }
        };
        constructors[GET_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheGetAllOperation();
            }
        };
        constructors[GET_ALL_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheGetAllOperationFactory();
            }
        };
        constructors[LOAD_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheLoadAllOperation();
            }
        };
        constructors[LOAD_ALL_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheLoadAllOperationFactory();
            }
        };
//        constructors[EVENT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
//            public IdentifiedDataSerializable createNew(Integer arg) {
////                        return new CacheEntryEventImpl<>();
//            }
//        };
        constructors[KEY_ITERATOR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheKeyIteratorOperation();
            }
        };
        constructors[KEY_ITERATION_RESULT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheKeyIteratorResult();
            }
        };
        constructors[ENTRY_PROCESSOR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheEntryProcessorOperation();
            }
        };
        constructors[CLEAR_RESPONSE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheClearResponse();
            }
        };
        constructors[CREATE_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheCreateConfigOperation();
            }
        };
        constructors[GET_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheGetConfigOperation();
            }

        };
        constructors[MANAGEMENT_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheManagementConfigOperation();
            }
        };
        constructors[LISTENER_REGISTRATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheListenerRegistrationOperation();
            }
        };
        return new ArrayDataSerializableFactory(constructors);
    }
}
