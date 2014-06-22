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

package com.hazelcast.map;

import com.hazelcast.map.client.*;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;

/**
 * @author mdogan 5/2/13
 */
public class MapPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.MAP_PORTABLE_FACTORY, -10);
    public static final int GET = 1;
    public static final int PUT = 2;
    public static final int PUT_IF_ABSENT = 3;
    public static final int TRY_PUT = 4;
    public static final int PUT_TRANSIENT = 5;
    public static final int SET = 6;
    public static final int CONTAINS_KEY = 7;
    public static final int CONTAINS_VALUE = 8;
    public static final int REMOVE = 9;
    public static final int REMOVE_IF_SAME = 10;
    public static final int DELETE = 11;
    public static final int FLUSH = 12;
    public static final int GET_ALL = 13;
    public static final int TRY_REMOVE = 14;
    public static final int REPLACE = 15;
    public static final int REPLACE_IF_SAME = 16;
    public static final int LOCK = 17;
    public static final int IS_LOCKED = 18;
    public static final int UNLOCK = 20;
    public static final int EVICT = 21;
    public static final int ADD_INTERCEPTOR = 23;
    public static final int REMOVE_INTERCEPTOR = 24;
    public static final int ADD_ENTRY_LISTENER = 25;
    public static final int ADD_ENTRY_LISTENER_SQL = 26;
    public static final int GET_ENTRY_VIEW = 27;
    public static final int ADD_INDEX = 28;
    public static final int KEY_SET = 29;
    public static final int VALUES = 30;
    public static final int ENTRY_SET = 31;
    public static final int SIZE = 33;
    public static final int QUERY = 34;
    public static final int SQL_QUERY = 35;
    public static final int CLEAR = 36;
    public static final int GET_LOCAL_MAP_STATS = 37;
    public static final int EXECUTE_ON_KEY = 38;
    public static final int EXECUTE_ON_ALL_KEYS = 39;
    public static final int PUT_ALL = 40;
    public static final int TXN_REQUEST = 41;
    public static final int TXN_REQUEST_WITH_SQL_QUERY = 42;
    public static final int EXECUTE_WITH_PREDICATE = 43;
    public static final int REMOVE_ENTRY_LISTENER = 44;
    public static final int EXECUTE_ON_KEYS = 45;
    public static final int EVICT_ALL = 46;
    public static final int LOAD_ALL_GIVEN_KEYS = 47;
    public static final int LOAD_ALL_KEYS = 48;
    public static final int IS_EMPTY = 49;

    public int getFactoryId() {
        return F_ID;
    }

    public PortableFactory createFactory() {
        return new PortableFactory() {
            final ConstructorFunction<Integer, Portable> constructors[] = new ConstructorFunction[IS_EMPTY + 1];
            {
                constructors[GET] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapGetRequest();
                    }
                };

                constructors[PUT] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapPutRequest();
                    }
                };

                constructors[PUT_IF_ABSENT] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapPutIfAbsentRequest();
                    }
                };

                constructors[TRY_PUT] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapTryPutRequest();
                    }
                };

                constructors[PUT_TRANSIENT] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapPutTransientRequest();
                    }
                };

                constructors[SET] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapSetRequest();
                    }
                };

                constructors[CONTAINS_KEY] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapContainsKeyRequest();
                    }
                };

                constructors[CONTAINS_VALUE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapContainsValueRequest();
                    }
                };

                constructors[REMOVE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapRemoveRequest();
                    }
                };

                constructors[REMOVE_IF_SAME] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapRemoveIfSameRequest();
                    }
                };

                constructors[DELETE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapDeleteRequest();
                    }
                };

                constructors[FLUSH] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapFlushRequest();
                    }
                };

                constructors[GET_ALL] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapGetAllRequest();
                    }
                };

                constructors[TRY_REMOVE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapTryRemoveRequest();
                    }
                };

                constructors[REPLACE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapReplaceRequest();
                    }
                };

                constructors[REPLACE_IF_SAME] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapReplaceIfSameRequest();
                    }
                };

                constructors[LOCK] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapLockRequest();
                    }
                };

                constructors[IS_LOCKED] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapIsLockedRequest();
                    }
                };

                constructors[UNLOCK] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapUnlockRequest();
                    }
                };

                constructors[EVICT] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapEvictRequest();
                    }
                };

                constructors[ADD_INTERCEPTOR] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapAddInterceptorRequest();
                    }
                };

                constructors[REMOVE_INTERCEPTOR] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapRemoveInterceptorRequest();
                    }
                };

                constructors[ADD_ENTRY_LISTENER] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapAddEntryListenerRequest();
                    }
                };

                constructors[ADD_ENTRY_LISTENER_SQL] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapAddEntryListenerSqlRequest();
                    }
                };


                constructors[GET_ENTRY_VIEW] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapGetEntryViewRequest();
                    }
                };

                constructors[ADD_INDEX] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapAddIndexRequest();
                    }
                };

                constructors[KEY_SET] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapKeySetRequest();
                    }
                };

                constructors[VALUES] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapValuesRequest();
                    }
                };

                constructors[ENTRY_SET] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapEntrySetRequest();
                    }
                };

                constructors[SIZE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapSizeRequest();
                    }
                };

                constructors[CLEAR] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapClearRequest();
                    }
                };

                constructors[QUERY] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapQueryRequest();
                    }
                };

                constructors[SQL_QUERY] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapSQLQueryRequest();
                    }
                };

                constructors[EXECUTE_ON_KEY] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapExecuteOnKeyRequest();
                    }
                };

                constructors[EXECUTE_ON_ALL_KEYS] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapExecuteOnAllKeysRequest();
                    }
                };

                constructors[PUT_ALL] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapPutAllRequest();
                    }
                };

                constructors[TXN_REQUEST] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new TxnMapRequest();
                    }
                };

                constructors[TXN_REQUEST_WITH_SQL_QUERY] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new TxnMapRequestWithSQLQuery();
                    }
                };

                constructors[EXECUTE_WITH_PREDICATE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapExecuteWithPredicateRequest();
                    }
                };
                constructors[EXECUTE_ON_KEYS] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapExecuteOnKeysRequest();
                    }
                };

                constructors[REMOVE_ENTRY_LISTENER] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapRemoveEntryListenerRequest();
                    }
                };

                constructors[EVICT_ALL] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapEvictAllRequest();
                    }
                };

                constructors[IS_EMPTY] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapIsEmptyRequest();
                    }
                };
                
                constructors[LOAD_ALL_GIVEN_KEYS] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapLoadGivenKeysRequest();
                    }
                };
                constructors[LOAD_ALL_KEYS] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new MapLoadAllKeysRequest();
                    }
                };
            }

            public Portable create(int classId) {
                return (classId > 0 && classId <= constructors.length) ? constructors[classId].createNew(classId) : null;
            }
        };
    }

    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
