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

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;

/**
 * @author enesakar 2/11/14
 */
public class CachePortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.CACHE_PORTABLE_FACTORY, -24);
    public static final int GET = 1;
    public static final int PUT = 2;
    public static final int PUT_IF_ABSENT = 3;
    public static final int REMOVE = 4;
    public static final int GET_AND_REMOVE = 5;
    public static final int REPLACE = 6;
    public static final int GET_AND_REPLACE = 7;
    public static final int SIZE = 8;
    public static final int CLEAR = 9;
    public static final int CONTAINS_KEY = 10;
    public static final int ADD_INVALIDATION_LISTENER = 11;
    public static final int INVALIDATION_MESSAGE = 12;
    public static final int REMOVE_INVALIDATION_LISTENER = 13;
    public static final int SEND_STATS = 14;

    public int getFactoryId() {
        return F_ID;
    }

    public PortableFactory createFactory() {
        return new PortableFactory() {
            final ConstructorFunction<Integer, Portable> constructors[] = new ConstructorFunction[16];

            {
//                constructors[GET] = new ConstructorFunction<Integer, Portable>() {
//                    public Portable createNew(Integer arg) {
//                        return new CacheGetRequest();
//                    }
//                };
//                constructors[PUT] = new ConstructorFunction<Integer, Portable>() {
//                    public Portable createNew(Integer arg) {
//                        return new CachePutRequest();
//                    }
//                };
//                constructors[PUT_IF_ABSENT] = new ConstructorFunction<Integer, Portable>() {
//                    public Portable createNew(Integer arg) {
//                        return new CachePutIfAbsentRequest();
//                    }
//                };
//                constructors[CONTAINS_KEY] = new ConstructorFunction<Integer, Portable>() {
//                    public Portable createNew(Integer arg) {
//                        return new CacheContainsKeyRequest();
//                    }
//                };
//                constructors[REPLACE] = new ConstructorFunction<Integer, Portable>() {
//                    public Portable createNew(Integer arg) {
//                        return new CacheReplaceRequest();
//                    }
//                };
//                constructors[GET_AND_REPLACE] = new ConstructorFunction<Integer, Portable>() {
//                    public Portable createNew(Integer arg) {
//                        return new CacheGetAndReplaceRequest();
//                    }
//                };
//                constructors[REMOVE] = new ConstructorFunction<Integer, Portable>() {
//                    public Portable createNew(Integer arg) {
//                        return new CacheRemoveRequest();
//                    }
//                };
//                constructors[GET_AND_REMOVE] = new ConstructorFunction<Integer, Portable>() {
//                    public Portable createNew(Integer arg) {
//                        return new CacheGetAndRemoveRequest();
//                    }
//                };
//                constructors[SIZE] = new ConstructorFunction<Integer, Portable>() {
//                    public Portable createNew(Integer arg) {
//                        return new CacheSizeRequest();
//                    }
//                };
//                constructors[CLEAR] = new ConstructorFunction<Integer, Portable>() {
//                    public Portable createNew(Integer arg) {
//                        return new CacheClearRequest();
//                    }
//                };
//                constructors[ADD_INVALIDATION_LISTENER] = new ConstructorFunction<Integer, Portable>() {
//                    public Portable createNew(Integer arg) {
//                        return new CacheAddInvalidationListenerRequest();
//                    }
//                };
//                constructors[INVALIDATION_MESSAGE] = new ConstructorFunction<Integer, Portable>() {
//                    public Portable createNew(Integer arg) {
//                        return new CacheInvalidationMessage();
//                    }
//                };
//                constructors[REMOVE_INVALIDATION_LISTENER] = new ConstructorFunction<Integer, Portable>() {
//                    public Portable createNew(Integer arg) {
//                        return new CacheRemoveInvalidationListenerRequest();
//                    }
//                };
//                constructors[SEND_STATS] = new ConstructorFunction<Integer, Portable>() {
//                    public Portable createNew(Integer arg) {
//                        return new CachePushStatsRequest();
//                    }
//                };
            }

            public Portable create(int classId) {
                if (constructors[classId] == null) {
                    throw new IllegalArgumentException("No registered constructor with class id:" + classId);
                }
                return (classId > 0 && classId <= constructors.length) ? constructors[classId].createNew(classId) : null;
            }
        };
    }

    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
