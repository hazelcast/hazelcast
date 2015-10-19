/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.multimap.impl;

import com.hazelcast.internal.serialization.PortableHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.multimap.impl.client.AddEntryListenerRequest;
import com.hazelcast.multimap.impl.client.ClearRequest;
import com.hazelcast.multimap.impl.client.ContainsRequest;
import com.hazelcast.multimap.impl.client.CountRequest;
import com.hazelcast.multimap.impl.client.EntrySetRequest;
import com.hazelcast.multimap.impl.client.GetAllRequest;
import com.hazelcast.multimap.impl.client.KeyBasedContainsRequest;
import com.hazelcast.multimap.impl.client.KeySetRequest;
import com.hazelcast.multimap.impl.client.MultiMapIsLockedRequest;
import com.hazelcast.multimap.impl.client.MultiMapLockRequest;
import com.hazelcast.multimap.impl.client.MultiMapUnlockRequest;
import com.hazelcast.multimap.impl.client.PortableEntrySetResponse;
import com.hazelcast.multimap.impl.client.PutRequest;
import com.hazelcast.multimap.impl.client.RemoveAllRequest;
import com.hazelcast.multimap.impl.client.RemoveEntryListenerRequest;
import com.hazelcast.multimap.impl.client.RemoveRequest;
import com.hazelcast.multimap.impl.client.SizeRequest;
import com.hazelcast.multimap.impl.client.TxnMultiMapGetRequest;
import com.hazelcast.multimap.impl.client.TxnMultiMapPutRequest;
import com.hazelcast.multimap.impl.client.TxnMultiMapRemoveAllRequest;
import com.hazelcast.multimap.impl.client.TxnMultiMapRemoveRequest;
import com.hazelcast.multimap.impl.client.TxnMultiMapSizeRequest;
import com.hazelcast.multimap.impl.client.TxnMultiMapValueCountRequest;
import com.hazelcast.multimap.impl.client.ValuesRequest;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

import java.util.Collection;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.MULTIMAP_PORTABLE_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.MULTIMAP_PORTABLE_FACTORY_ID;

public class MultiMapPortableHook implements PortableHook {
    public static final int F_ID = FactoryIdHelper.getFactoryId(MULTIMAP_PORTABLE_FACTORY, MULTIMAP_PORTABLE_FACTORY_ID);
    public static final int CLEAR = 1;
    public static final int CONTAINS_ENTRY = 2;
    public static final int COUNT = 3;
    public static final int ENTRY_SET = 4;
    public static final int GET_ALL = 5;
    public static final int GET = 6;
    public static final int KEY_SET = 7;
    public static final int PUT = 8;
    public static final int REMOVE_ALL = 9;
    public static final int REMOVE = 10;
    public static final int SET = 11;
    public static final int SIZE = 12;
    public static final int VALUES = 13;
    public static final int ADD_ENTRY_LISTENER = 14;
    public static final int ENTRY_SET_RESPONSE = 15;
    public static final int LOCK = 16;
    public static final int UNLOCK = 17;
    public static final int IS_LOCKED = 18;
    public static final int TXN_MM_PUT = 19;
    public static final int TXN_MM_GET = 20;
    public static final int TXN_MM_REMOVE = 21;
    public static final int TXN_MM_VALUE_COUNT = 22;
    public static final int TXN_MM_SIZE = 23;
    public static final int REMOVE_ENTRY_LISTENER = 24;
    public static final int TXN_MM_REMOVEALL = 25;
    public static final int KEY_BASED_CONTAINS = 26;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    public PortableFactory createFactory() {
        return new PortableFactory() {
            @Override
            public Portable create(int classId) {
                switch (classId) {
                    case CLEAR:
                        return new ClearRequest();
                    case CONTAINS_ENTRY:
                        return new ContainsRequest();
                    case COUNT:
                        return new CountRequest();
                    case ENTRY_SET:
                        return new EntrySetRequest();
                    case GET_ALL:
                        return new GetAllRequest();
                    case KEY_SET:
                        return new KeySetRequest();
                    case PUT:
                        return new PutRequest();
                    case REMOVE_ALL:
                        return new RemoveAllRequest();
                    case REMOVE:
                        return new RemoveRequest();
                    case SIZE:
                        return new SizeRequest();
                    case VALUES:
                        return new ValuesRequest();
                    case ADD_ENTRY_LISTENER:
                        return new AddEntryListenerRequest();
                    case ENTRY_SET_RESPONSE:
                        return new PortableEntrySetResponse();
                    case LOCK:
                        return new MultiMapLockRequest();
                    case UNLOCK:
                        return new MultiMapUnlockRequest();
                    case IS_LOCKED:
                        return new MultiMapIsLockedRequest();
                    case TXN_MM_PUT:
                        return new TxnMultiMapPutRequest();
                    case TXN_MM_GET:
                        return new TxnMultiMapGetRequest();
                    case TXN_MM_REMOVE:
                        return new TxnMultiMapRemoveRequest();
                    case TXN_MM_VALUE_COUNT:
                        return new TxnMultiMapValueCountRequest();
                    case TXN_MM_SIZE:
                        return new TxnMultiMapSizeRequest();
                    case REMOVE_ENTRY_LISTENER:
                        return new RemoveEntryListenerRequest();
                    case TXN_MM_REMOVEALL:
                        return new TxnMultiMapRemoveAllRequest();
                    case KEY_BASED_CONTAINS:
                        return new KeyBasedContainsRequest();
                    default:
                        return null;
                }
            }
        };
    }

    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
