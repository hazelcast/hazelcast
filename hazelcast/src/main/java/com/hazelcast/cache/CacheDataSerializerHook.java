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

import com.hazelcast.cache.operation.CacheClearOperation;
import com.hazelcast.cache.operation.CacheClearOperationFactory;
import com.hazelcast.cache.operation.CacheContainsKeyOperation;
import com.hazelcast.cache.operation.CacheGetAllOperation;
import com.hazelcast.cache.operation.CacheGetAllOperationFactory;
import com.hazelcast.cache.operation.CacheGetAndRemoveOperation;
import com.hazelcast.cache.operation.CacheGetAndReplaceOperation;
import com.hazelcast.cache.operation.CacheGetOperation;
import com.hazelcast.cache.operation.CacheKeyIteratorOperation;
import com.hazelcast.cache.operation.CachePutBackupOperation;
import com.hazelcast.cache.operation.CachePutIfAbsentOperation;
import com.hazelcast.cache.operation.CachePutOperation;
import com.hazelcast.cache.operation.CacheRemoveBackupOperation;
import com.hazelcast.cache.operation.CacheRemoveOperation;
import com.hazelcast.cache.operation.CacheReplaceOperation;
import com.hazelcast.cache.operation.CacheSizeOperation;
import com.hazelcast.cache.operation.CacheSizeOperationFactory;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public final class CacheDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.CACHE_DS_FACTORY, -25);

    public static final short GET = 0;
    public static final short CONTAINS_KEY = 1;
    public static final short PUT = 2;
    public static final short PUT_IF_ABSENT = 3;
    public static final short REMOVE = 4;
    public static final short GET_AND_REMOVE = 5;
    public static final short REPLACE = 6;
    public static final short GET_AND_REPLACE = 7;

    public static final short PUT_BACKUP = 8;
    public static final short REMOVE_BACKUP = 9;

    public static final short SIZE = 10;
    public static final short SIZE_FACTORY = 11;
    public static final short CLEAR = 12;
    public static final short CLEAR_FACTORY = 13;
    public static final short GET_STATS = 14;
    public static final short GET_STATS_FACTORY = 15;
    public static final short GET_ALL = 16;
    public static final short GET_ALL_FACTORY = 17;
    public static final short EXPIRY_POLICY = 18;
    public static final short EVENT = 19;
    public static final short KEY_ITERATOR = 20;
    public static final short KEY_ITERATION_RESULT = 21;


    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {

            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case GET:
                        return new CacheGetOperation();
                    case CONTAINS_KEY:
                        return new CacheContainsKeyOperation();
                    case PUT:
                        return new CachePutOperation();
                    case PUT_IF_ABSENT:
                        return new CachePutIfAbsentOperation();
                    case REMOVE:
                        return new CacheRemoveOperation();
                    case GET_AND_REMOVE:
                        return new CacheGetAndRemoveOperation();
                    case REPLACE:
                        return new CacheReplaceOperation();
                    case GET_AND_REPLACE:
                        return new CacheGetAndReplaceOperation();
                    case PUT_BACKUP:
                        return new CachePutBackupOperation();
                    case REMOVE_BACKUP:
                        return new CacheRemoveBackupOperation();
                    case SIZE:
                        return new CacheSizeOperation();
                    case SIZE_FACTORY:
                        return new CacheSizeOperationFactory();
                    case CLEAR:
                        return new CacheClearOperation();
                    case CLEAR_FACTORY:
                        return new CacheClearOperationFactory();
                    case GET_STATS:
//                        return new CacheXXXXXOperation();
                    case GET_STATS_FACTORY:
//                        return new CacheXXXXXOperation();
                    case GET_ALL:
                        return new CacheGetAllOperation();
                    case GET_ALL_FACTORY:
                        return new CacheGetAllOperationFactory();
                    case EVENT:
//                        return new CacheEntryEventImpl<>();
                    case KEY_ITERATOR:
                        return new CacheKeyIteratorOperation();
                    case KEY_ITERATION_RESULT:
                        return new CacheKeyIteratorResult();
                }
                throw new IllegalArgumentException("Unknown type-id: " + typeId);
            }
        };
    }
}
