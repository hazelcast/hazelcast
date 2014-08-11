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

package com.hazelcast.multimap.impl;

import com.hazelcast.multimap.impl.operations.client.AddEntryListenerRequest;
import com.hazelcast.multimap.impl.operations.client.ClearRequest;
import com.hazelcast.multimap.impl.operations.client.ContainsEntryRequest;
import com.hazelcast.multimap.impl.operations.client.CountRequest;
import com.hazelcast.multimap.impl.operations.client.EntrySetRequest;
import com.hazelcast.multimap.impl.operations.client.GetAllRequest;
import com.hazelcast.multimap.impl.operations.client.KeySetRequest;
import com.hazelcast.multimap.impl.operations.client.MultiMapIsLockedRequest;
import com.hazelcast.multimap.impl.operations.client.MultiMapLockRequest;
import com.hazelcast.multimap.impl.operations.client.MultiMapUnlockRequest;
import com.hazelcast.multimap.impl.operations.client.PortableEntrySetResponse;
import com.hazelcast.multimap.impl.operations.client.PutRequest;
import com.hazelcast.multimap.impl.operations.client.RemoveAllRequest;
import com.hazelcast.multimap.impl.operations.client.RemoveEntryListenerRequest;
import com.hazelcast.multimap.impl.operations.client.RemoveRequest;
import com.hazelcast.multimap.impl.operations.client.SizeRequest;
import com.hazelcast.multimap.impl.operations.client.TxnMultiMapGetRequest;
import com.hazelcast.multimap.impl.operations.client.TxnMultiMapPutRequest;
import com.hazelcast.multimap.impl.operations.client.TxnMultiMapRemoveAllRequest;
import com.hazelcast.multimap.impl.operations.client.TxnMultiMapRemoveRequest;
import com.hazelcast.multimap.impl.operations.client.TxnMultiMapSizeRequest;
import com.hazelcast.multimap.impl.operations.client.TxnMultiMapValueCountRequest;
import com.hazelcast.multimap.impl.operations.client.ValuesRequest;
import com.hazelcast.nio.serialization.ArrayPortableFactory;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;
import com.hazelcast.util.ConstructorFunction;
import java.util.Collection;

public class MultiMapPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.MULTIMAP_PORTABLE_FACTORY, -12);

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


    public int getFactoryId() {
        return F_ID;
    }

    public PortableFactory createFactory() {
        ConstructorFunction<Integer, Portable>[] constructors = new ConstructorFunction[TXN_MM_REMOVEALL + 1];
        constructors[CLEAR] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new ClearRequest();
            }
        };
        constructors[CONTAINS_ENTRY] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new ContainsEntryRequest();
            }
        };
        constructors[COUNT] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new CountRequest();
            }
        };
        constructors[ENTRY_SET] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new EntrySetRequest();
            }
        };
        constructors[GET_ALL] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new GetAllRequest();
            }
        };
        constructors[KEY_SET] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new KeySetRequest();
            }
        };
        constructors[PUT] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new PutRequest();
            }
        };
        constructors[REMOVE_ALL] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new RemoveAllRequest();
            }
        };
        constructors[REMOVE] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new RemoveRequest();
            }
        };
        constructors[SIZE] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new SizeRequest();
            }
        };
        constructors[VALUES] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new ValuesRequest();
            }
        };
        constructors[ADD_ENTRY_LISTENER] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new AddEntryListenerRequest();
            }
        };
        constructors[ENTRY_SET_RESPONSE] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new PortableEntrySetResponse();
            }
        };
        constructors[LOCK] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new MultiMapLockRequest();
            }
        };
        constructors[UNLOCK] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new MultiMapUnlockRequest();
            }
        };
        constructors[IS_LOCKED] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new MultiMapIsLockedRequest();
            }
        };
        constructors[TXN_MM_PUT] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new TxnMultiMapPutRequest();
            }
        };
        constructors[TXN_MM_GET] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new TxnMultiMapGetRequest();
            }
        };
        constructors[TXN_MM_REMOVE] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new TxnMultiMapRemoveRequest();
            }
        };
        constructors[TXN_MM_VALUE_COUNT] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new TxnMultiMapValueCountRequest();
            }
        };
        constructors[TXN_MM_SIZE] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new TxnMultiMapSizeRequest();
            }
        };
        constructors[REMOVE_ENTRY_LISTENER] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new RemoveEntryListenerRequest();
            }
        };
        constructors[TXN_MM_REMOVEALL] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new TxnMultiMapRemoveAllRequest();
            }
        };


        return new ArrayPortableFactory(constructors);
    }

    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
