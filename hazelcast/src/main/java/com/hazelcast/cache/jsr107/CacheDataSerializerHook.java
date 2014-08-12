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

package com.hazelcast.cache.jsr107;

import com.hazelcast.cache.jsr107.operation.CacheClearBackupOperation;
import com.hazelcast.cache.jsr107.operation.CacheClearOperation;
import com.hazelcast.cache.jsr107.operation.CacheClearOperationFactory;
import com.hazelcast.cache.jsr107.operation.CacheContainsKeyOperation;
import com.hazelcast.cache.jsr107.operation.CacheEntryProcessorOperation;
import com.hazelcast.cache.jsr107.operation.CacheGetAllOperation;
import com.hazelcast.cache.jsr107.operation.CacheGetAllOperationFactory;
import com.hazelcast.cache.jsr107.operation.CacheGetAndRemoveOperation;
import com.hazelcast.cache.jsr107.operation.CacheGetAndReplaceOperation;
import com.hazelcast.cache.jsr107.operation.CacheGetOperation;
import com.hazelcast.cache.jsr107.operation.CacheKeyIteratorOperation;
import com.hazelcast.cache.jsr107.operation.CacheLoadAllOperation;
import com.hazelcast.cache.jsr107.operation.CacheLoadAllOperationFactory;
import com.hazelcast.cache.jsr107.operation.CachePutAllBackupOperation;
import com.hazelcast.cache.jsr107.operation.CachePutBackupOperation;
import com.hazelcast.cache.jsr107.operation.CachePutIfAbsentOperation;
import com.hazelcast.cache.jsr107.operation.CachePutOperation;
import com.hazelcast.cache.jsr107.operation.CacheRemoveBackupOperation;
import com.hazelcast.cache.jsr107.operation.CacheRemoveOperation;
import com.hazelcast.cache.jsr107.operation.CacheReplaceOperation;
import com.hazelcast.cache.jsr107.operation.CacheSizeOperation;
import com.hazelcast.cache.jsr107.operation.CacheSizeOperationFactory;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

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
    public static final short GET_STATS = i++;
    public static final short GET_STATS_FACTORY = i++;
    public static final short GET_ALL = i++;
    public static final short GET_ALL_FACTORY = i++;
    public static final short LOAD_ALL = i++;
    public static final short LOAD_ALL_FACTORY = i++;

    public static final short EXPIRY_POLICY = i++;
    public static final short EVENT = i++;
    public static final short KEY_ITERATOR = i++;
    public static final short KEY_ITERATION_RESULT = i++;
    public static final short ENTRY_PROCESSOR = i++;
    public static final short CLEAR_RESPONSE = i++;


    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            public IdentifiedDataSerializable create(int typeId) {
                if (typeId == GET) {
                    return new CacheGetOperation();
                } else if (typeId == CONTAINS_KEY) {
                    return new CacheContainsKeyOperation();
                } else if (typeId == PUT) {
                    return new CachePutOperation();
                } else if (typeId == PUT_IF_ABSENT) {
                    return new CachePutIfAbsentOperation();
                } else if (typeId == REMOVE) {
                    return new CacheRemoveOperation();
                } else if (typeId == GET_AND_REMOVE) {
                    return new CacheGetAndRemoveOperation();
                } else if (typeId == REPLACE) {
                    return new CacheReplaceOperation();
                } else if (typeId == GET_AND_REPLACE) {
                    return new CacheGetAndReplaceOperation();
                } else if (typeId == PUT_BACKUP) {
                    return new CachePutBackupOperation();
                } else if (typeId == PUT_ALL_BACKUP) {
                    return new CachePutAllBackupOperation();
                } else if (typeId == REMOVE_BACKUP) {
                    return new CacheRemoveBackupOperation();
                } else if (typeId == CLEAR_BACKUP) {
                    return new CacheClearBackupOperation();
                } else if (typeId == SIZE) {
                    return new CacheSizeOperation();
                } else if (typeId == SIZE_FACTORY) {
                    return new CacheSizeOperationFactory();
                } else if (typeId == CLEAR) {
                    return new CacheClearOperation();
                } else if (typeId == CLEAR_FACTORY) {
                    return new CacheClearOperationFactory();
                } else if (typeId == GET_STATS) {
//                        return new CacheXXXXXOperation();
                } else if (typeId == GET_STATS_FACTORY) {
//                        return new CacheXXXXXOperation();
                } else if (typeId == GET_ALL) {
                    return new CacheGetAllOperation();
                } else if (typeId == GET_ALL_FACTORY) {
                    return new CacheGetAllOperationFactory();
                } else if (typeId == LOAD_ALL) {
                    return new CacheLoadAllOperation();
                } else if (typeId == LOAD_ALL_FACTORY) {
                    return new CacheLoadAllOperationFactory();
                } else if (typeId == EVENT) {
//                        return new CacheEntryEventImpl<>();
                } else if (typeId == KEY_ITERATOR) {
                    return new CacheKeyIteratorOperation();
                } else if (typeId == KEY_ITERATION_RESULT) {
                    return new CacheKeyIteratorResult();
                } else if (typeId == ENTRY_PROCESSOR) {
                    return new CacheEntryProcessorOperation();
                } else if (typeId == CLEAR_RESPONSE) {
                    return new CacheClearResponse();
                }
                throw new IllegalArgumentException("Unknown type-id: " + typeId);
            }
        };
    }
}
