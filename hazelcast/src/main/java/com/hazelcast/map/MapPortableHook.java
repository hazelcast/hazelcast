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

import com.hazelcast.map.clientv2.*;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;

/**
 * @mdogan 5/2/13
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
    public static final int TRY_LOCK = 19;
    public static final int UNLOCK = 20;
    public static final int FORCE_UNLOCK = 21;
    public static final int ADD_LOCAL_ENTRY_LISTENER = 22;
    public static final int ADD_INTERCEPTOR = 23;
    public static final int REMOVE_INTERCEPTOR = 24;
    public static final int ADD_ENTRY_LISTENER = 25;
    public static final int REMOVE_ENTRY_LISTENER = 26;
    public static final int GET_ENTRY_VIEW = 27;
    public static final int EVICT = 28;
    public static final int KEYSET = 29;
    public static final int VALUES = 30;
    public static final int ENTRY_SET = 31;
    public static final int KEYSET_QUERY = 32;
    public static final int ENTRY_SET_QUERY = 33;
    public static final int VALUES_QUERY = 34;
    public static final int LOCAL_KEYSET = 35;
    public static final int LOCAL_KEYSET_QUERY = 36;
    public static final int ADD_INDEX = 37;
    public static final int GET_LOCAL_MAP_STATS = 38;
    public static final int EXECUTE_ON_KEY = 39;
    public static final int EXECUTE_ON_ALL_KEYS = 40;



    public int getFactoryId() {
        return F_ID;
    }

    public PortableFactory createFactory() {
        return new PortableFactory() {
            public Portable create(int classId) {
                switch (classId) {
                    case GET:
                        return new MapGetRequest();
                    case PUT:
                        return new MapPutRequest();
                    case PUT_IF_ABSENT:
                        return new MapPutIfAbsentRequest();
                    case TRY_PUT:
                        return new MapTryPutRequest();
                    case PUT_TRANSIENT:
                        return new MapPutTransientRequest();
                    case SET:
                        return new MapSetRequest();
                    case CONTAINS_KEY:
                        return new MapContainsKeyRequest();
                    case CONTAINS_VALUE:
                        return new MapContainsValueRequest();
                    case REMOVE:
                        return new MapRemoveRequest();
                    case REMOVE_IF_SAME:
                        return new MapRemoveIfSameRequest();
                    case DELETE:
                        return new MapDeleteRequest();
                    case FLUSH:
                        return new MapFlushRequest();
                    case GET_ALL:
                        return new MapGetAllRequest();
                    case TRY_REMOVE:
                        return new MapTryRemoveRequest();
                    case REPLACE:
                        return new MapReplaceRequest();
                    case REPLACE_IF_SAME:
                        return new MapReplaceIfSameRequest();
                    case LOCK:
                        return new MapLockRequest();
                    case IS_LOCKED:
                        return new MapIsLockedRequest();
                    case TRY_LOCK:
                        return new MapTryLockRequest();
                    case UNLOCK:
                        return new MapUnlockRequest();
                    case FORCE_UNLOCK:
                        return new MapForceUnlockRequest();
                    case ADD_LOCAL_ENTRY_LISTENER:
                        return new MapAddLocalEntryListenerRequest();
                    case ADD_INTERCEPTOR:
                        return new MapAddInterceptorRequest();
                    case REMOVE_INTERCEPTOR:
                        return new MapRemoveInterceptorRequest();
                    case ADD_ENTRY_LISTENER:
                        return new MapAddEntryListenerRequest();
                    case REMOVE_ENTRY_LISTENER:
                        return new MapRemoveEntryListenerRequest();
                    case GET_ENTRY_VIEW:
                        return new MapGetEntryViewRequest();
                    case EVICT:
                        return new MapEvictRequest();
                    case KEYSET:
                        return new MapKeysetRequest();
                    case VALUES:
                        return new MapValuesRequest();
                    case ENTRY_SET:
                        return new MapEntrySetRequest();
                    case KEYSET_QUERY:
                        return new MapKeysetQueryRequest();
                    case ENTRY_SET_QUERY:
                        return new MapEntrySetQueryRequest();
                    case VALUES_QUERY:
                        return new MapValuesQueryRequest();
                    case LOCAL_KEYSET:
                        return new MapLocalKeysetRequest();
                    case LOCAL_KEYSET_QUERY:
                        return new MapLocalKeysetQueryRequest();
                    case ADD_INDEX:
                        return new MapAddIndexRequest();
                    case GET_LOCAL_MAP_STATS:
                        return new MapGetLocalMapStatsRequest();
                    case EXECUTE_ON_KEY:
                        return new MapExecuteOnKeyRequest();
                    case EXECUTE_ON_ALL_KEYS:
                        return new MapExecuteOnAllKeysRequest();
                }
                return null;
            }
        };
    }
}
