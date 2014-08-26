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

import com.hazelcast.cache.client.CacheClearRequest;
import com.hazelcast.cache.client.CacheContainsKeyRequest;
import com.hazelcast.cache.client.CacheCreateConfigRequest;
import com.hazelcast.cache.client.CacheGetAllRequest;
import com.hazelcast.cache.client.CacheGetAndRemoveRequest;
import com.hazelcast.cache.client.CacheGetAndReplaceRequest;
import com.hazelcast.cache.client.CacheGetConfigRequest;
import com.hazelcast.cache.client.CacheGetRequest;
import com.hazelcast.cache.client.CacheIterateRequest;
import com.hazelcast.cache.client.CacheLoadAllRequest;
import com.hazelcast.cache.client.CachePutIfAbsentRequest;
import com.hazelcast.cache.client.CachePutRequest;
import com.hazelcast.cache.client.CacheRemoveRequest;
import com.hazelcast.cache.client.CacheReplaceRequest;
import com.hazelcast.cache.client.CacheSizeRequest;
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
public class CachePortableHook
        implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.CACHE_PORTABLE_FACTORY, -24);
    public static final int GET = 2;
    public static final int PUT = 3;
    public static final int PUT_IF_ABSENT = 4;
    public static final int REMOVE = 5;
    public static final int GET_AND_REMOVE = 6;
    public static final int REPLACE = 7;
    public static final int GET_AND_REPLACE = 8;
    public static final int SIZE = 9;
    public static final int CLEAR = 10;
    public static final int CONTAINS_KEY = 11;
    public static final int ITERATE = 12;
    public static final int ADD_INVALIDATION_LISTENER = 13;
    public static final int INVALIDATION_MESSAGE = 14;
    public static final int REMOVE_INVALIDATION_LISTENER = 15;
    public static final int SEND_STATS = 16;
    public static final int CREATE_CONFIG = 17;
    public static final int GET_CONFIG = 18;
    public static final int GET_ALL = 19;
    public static final int LOAD_ALL = 20;

    public static final int LEN = 20;

    public int getFactoryId() {
        return F_ID;
    }

    public PortableFactory createFactory() {
        return new PortableFactory() {
            final ConstructorFunction<Integer, Portable>[] constructors = new ConstructorFunction[LEN];

            {
                constructors[GET] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheGetRequest();
                    }
                };
                constructors[PUT] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CachePutRequest();
                    }
                };
                constructors[PUT_IF_ABSENT] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CachePutIfAbsentRequest();
                    }
                };
                constructors[REMOVE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheRemoveRequest();
                    }
                };
                constructors[GET_AND_REMOVE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheGetAndRemoveRequest();
                    }
                };
                constructors[REPLACE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheReplaceRequest();
                    }
                };
                constructors[GET_AND_REPLACE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheGetAndReplaceRequest();
                    }
                };
                constructors[SIZE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheSizeRequest();
                    }
                };
                constructors[CLEAR] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheClearRequest();
                    }
                };
                constructors[CONTAINS_KEY] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheContainsKeyRequest();
                    }
                };
                constructors[ITERATE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheIterateRequest();
                    }
                };
                //                                constructors[ADD_INVALIDATION_LISTENER] =
                //                                         new ConstructorFunction<Integer, Portable>() {
                //                                    public Portable createNew(Integer arg) {
                //                                        return new CacheAddInvalidationListenerRequest();
                //                                    }
                //                                };
                //                                constructors[INVALIDATION_MESSAGE] =
                //                                         new ConstructorFunction<Integer, Portable>() {
                //                                    public Portable createNew(Integer arg) {
                //                                        return new CacheInvalidationMessage();
                //                                    }
                //                                };
                //                                constructors[REMOVE_INVALIDATION_LISTENER] =
                //                                         new ConstructorFunction<Integer, Portable>() {
                //                                    public Portable createNew(Integer arg) {
                //                                        return new CacheRemoveInvalidationListenerRequest();
                //                                    }
                //                                };
                //                                constructors[SEND_STATS] =
                //                                          new ConstructorFunction<Integer, Portable>() {
                //                                    public Portable createNew(Integer arg) {
                //                                        return new CachePushStatsRequest();
                //                                    }
                //                                };
                constructors[CREATE_CONFIG] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheCreateConfigRequest();
                    }
                };
                constructors[GET_CONFIG] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheGetConfigRequest();
                    }
                };
                constructors[GET_ALL] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheGetAllRequest();
                    }
                };
                constructors[LOAD_ALL] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheLoadAllRequest();
                    }
                };
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
