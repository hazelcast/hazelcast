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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.impl.operation.CacheClearBackupOperation;
import com.hazelcast.cache.impl.operation.CacheClearOperation;
import com.hazelcast.cache.impl.operation.CacheClearOperationFactory;
import com.hazelcast.cache.impl.operation.CacheContainsKeyOperation;
import com.hazelcast.cache.impl.operation.CacheCreateConfigOperation;
import com.hazelcast.cache.impl.operation.CacheDestroyOperation;
import com.hazelcast.cache.impl.operation.CacheEntryProcessorOperation;
import com.hazelcast.cache.impl.operation.CacheGetAllOperation;
import com.hazelcast.cache.impl.operation.CacheGetAllOperationFactory;
import com.hazelcast.cache.impl.operation.CacheGetAndRemoveOperation;
import com.hazelcast.cache.impl.operation.CacheGetAndReplaceOperation;
import com.hazelcast.cache.impl.operation.CacheGetConfigOperation;
import com.hazelcast.cache.impl.operation.CacheGetOperation;
import com.hazelcast.cache.impl.operation.CacheKeyIteratorOperation;
import com.hazelcast.cache.impl.operation.CacheListenerRegistrationOperation;
import com.hazelcast.cache.impl.operation.CacheLoadAllOperation;
import com.hazelcast.cache.impl.operation.CacheLoadAllOperationFactory;
import com.hazelcast.cache.impl.operation.CacheManagementConfigOperation;
import com.hazelcast.cache.impl.operation.CachePutAllBackupOperation;
import com.hazelcast.cache.impl.operation.CachePutBackupOperation;
import com.hazelcast.cache.impl.operation.CachePutIfAbsentOperation;
import com.hazelcast.cache.impl.operation.CachePutOperation;
import com.hazelcast.cache.impl.operation.CacheRemoveBackupOperation;
import com.hazelcast.cache.impl.operation.CacheRemoveOperation;
import com.hazelcast.cache.impl.operation.CacheReplaceOperation;
import com.hazelcast.cache.impl.operation.CacheSizeOperation;
import com.hazelcast.cache.impl.operation.CacheSizeOperationFactory;
import com.hazelcast.nio.serialization.ArrayDataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;

/**
 * {@link CacheDataSerializerHook} contains all the ID hooks for {@link IdentifiedDataSerializable} classes used inside the
 * JCache framework.
 * <p>CacheProxy operations are mapped here. This factory class is used by internal serialization system to create
 * {@link IdentifiedDataSerializable} classes without using reflection.</p>
 */
public final class CacheDataSerializerHook
        implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.CACHE_DS_FACTORY, -25);
    public static final short GET = 1;
    public static final short CONTAINS_KEY = 2;
    public static final short PUT = 3;
    public static final short PUT_IF_ABSENT = 4;
    public static final short REMOVE = 5;
    public static final short GET_AND_REMOVE = 6;
    public static final short REPLACE = 7;
    public static final short GET_AND_REPLACE = 8;
    public static final short PUT_BACKUP = 9;
    public static final short PUT_ALL_BACKUP = 10;
    public static final short REMOVE_BACKUP = 11;
    public static final short CLEAR_BACKUP = 12;
    public static final short SIZE = 13;
    public static final short SIZE_FACTORY = 14;
    public static final short CLEAR = 15;
    public static final short CLEAR_FACTORY = 16;
    public static final short GET_ALL = 17;
    public static final short GET_ALL_FACTORY = 18;
    public static final short LOAD_ALL = 19;
    public static final short LOAD_ALL_FACTORY = 20;
    public static final short EXPIRY_POLICY = 21;
    public static final short KEY_ITERATOR = 22;
    public static final short KEY_ITERATION_RESULT = 23;
    public static final short ENTRY_PROCESSOR = 24;
    public static final short CLEAR_RESPONSE = 25;
    public static final short CREATE_CONFIG = 26;
    public static final short GET_CONFIG = 27;
    public static final short MANAGEMENT_CONFIG = 28;
    public static final short LISTENER_REGISTRATION = 29;
    public static final short DESTROY_CACHE = 30;
    public static final short CACHE_EVENT_DATA = 31;
    public static final short CACHE_EVENT_DATA_SET = 32;

    private static final int LEN = 33;

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
        constructors[DESTROY_CACHE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheDestroyOperation();
            }
        };
        constructors[CACHE_EVENT_DATA] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheEventDataImpl();
            }
        };
        constructors[CACHE_EVENT_DATA_SET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CacheEventSet();
            }
        };
        return new ArrayDataSerializableFactory(constructors);
    }
}
