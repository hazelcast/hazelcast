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

package com.hazelcast.multimap;

import com.hazelcast.multimap.operations.client.*;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;

/**
 * @author ali 5/9/13
 */
public class MultiMapPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.MULTIMAP_PORTABLE_FACTORY, -12);


    public static final int ADD_ALL = 1;
    public static final int CLEAR = 2;
    public static final int COMPARE_AND_REMOVE = 3;
    public static final int CONTAINS_ALL = 4;
    public static final int CONTAINS_ENTRY = 5;
    public static final int CONTAINS = 6;
    public static final int COUNT = 7;
    public static final int ENTRY_SET = 8;
    public static final int GET_ALL = 9;
    public static final int GET = 10;
    public static final int INDEX_OF = 11;
    public static final int KEY_SET = 12;
    public static final int PUT = 13;
    public static final int REMOVE_ALL = 14;
    public static final int REMOVE_INDEX = 15;
    public static final int REMOVE = 16;
    public static final int SET = 17;
    public static final int SIZE = 18;
    public static final int VALUES = 19;
    public static final int ADD_ENTRY_LISTENER = 20;
    public static final int ENTRY_SET_RESPONSE = 21;
    public static final int LOCK = 22;
    public static final int UNLOCK = 23;
    public static final int IS_LOCKED = 24;
    public static final int ADD_ITEM_LISTENER = 25;
    public static final int DESTROY = 26;

    public static final int TXN_MM_PUT = 27;
    public static final int TXN_MM_GET = 28;
    public static final int TXN_MM_REMOVE = 29;
    public static final int TXN_MM_VALUE_COUNT = 30;
    public static final int TXN_MM_SIZE = 31;

    public static final int TXN_LIST_ADD = 32;
    public static final int TXN_LIST_REMOVE = 33;
    public static final int TXN_LIST_SIZE = 34;

    public static final int TXN_SET_ADD = 35;
    public static final int TXN_SET_REMOVE = 36;
    public static final int TXN_SET_SIZE = 37;


    public int getFactoryId() {
        return F_ID;
    }

    public PortableFactory createFactory() {
        ConstructorFunction<Integer, Portable> constructors[] = new ConstructorFunction[TXN_SET_SIZE +1];
//        constructors[ADD_ALL] = new ConstructorFunction<Integer, Portable>() {
//            public Portable createNew(Integer arg) {
//                return new AddAllRequest();
//            }
//        };
        constructors[CLEAR] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new ClearRequest();
            }
        };
//        constructors[COMPARE_AND_REMOVE] = new ConstructorFunction<Integer, Portable>() {
//            public Portable createNew(Integer arg) {
//                return new CompareAndRemoveRequest();
//            }
//        };
//        constructors[CONTAINS_ALL] = new ConstructorFunction<Integer, Portable>() {
//            public Portable createNew(Integer arg) {
//                return new ContainsAllRequest();
//            }
//        };
        constructors[CONTAINS_ENTRY] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new ContainsEntryRequest();
            }
        };
//        constructors[CONTAINS] = new ConstructorFunction<Integer, Portable>() {
//            public Portable createNew(Integer arg) {
//                return new ContainsRequest();
//            }
//        };
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
//        constructors[GET] = new ConstructorFunction<Integer, Portable>() {
//            public Portable createNew(Integer arg) {
//                return new GetRequest();
//            }
//        };
//        constructors[INDEX_OF] = new ConstructorFunction<Integer, Portable>() {
//            public Portable createNew(Integer arg) {
//                return new IndexOfRequest();
//            }
//        };
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
//        constructors[REMOVE_INDEX] = new ConstructorFunction<Integer, Portable>() {
//            public Portable createNew(Integer arg) {
//                return new RemoveIndexRequest();
//            }
//        };
        constructors[REMOVE] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new RemoveRequest();
            }
        };
//        constructors[SET] = new ConstructorFunction<Integer, Portable>() {
//            public Portable createNew(Integer arg) {
//                return new SetRequest();
//            }
//        };
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
//        constructors[ADD_ITEM_LISTENER] = new ConstructorFunction<Integer, Portable>() {
//            public Portable createNew(Integer arg) {
//                return new AddItemListenerRequest();
//            }
//        };
        constructors[DESTROY] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new MultiMapDestroyRequest();
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

//        constructors[TXN_LIST_ADD] = new ConstructorFunction<Integer, Portable>() {
//            public Portable createNew(Integer arg) {
//                return new TxnListAddRequest();
//            }
//        };
//        constructors[TXN_LIST_REMOVE] = new ConstructorFunction<Integer, Portable>() {
//            public Portable createNew(Integer arg) {
//                return new TxnListRemoveRequest();
//            }
//        };
//        constructors[TXN_LIST_SIZE] = new ConstructorFunction<Integer, Portable>() {
//            public Portable createNew(Integer arg) {
//                return new TxnListSizeRequest();
//            }
//        };
//
//        constructors[TXN_SET_ADD] = new ConstructorFunction<Integer, Portable>() {
//            public Portable createNew(Integer arg) {
//                return new TxnSetAddRequest();
//            }
//        };
//        constructors[TXN_SET_REMOVE] = new ConstructorFunction<Integer, Portable>() {
//            public Portable createNew(Integer arg) {
//                return new TxnSetRemoveRequest();
//            }
//        };
//        constructors[TXN_SET_SIZE] = new ConstructorFunction<Integer, Portable>() {
//            public Portable createNew(Integer arg) {
//                return new TxnSetSizeRequest();
//            }
//        };

        return new ArrayPortableFactory(constructors);
    }

    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
