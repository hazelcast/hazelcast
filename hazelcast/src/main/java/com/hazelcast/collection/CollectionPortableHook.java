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

package com.hazelcast.collection;

import com.hazelcast.collection.client.CollectionAddAllRequest;
import com.hazelcast.collection.client.CollectionAddListenerRequest;
import com.hazelcast.collection.client.CollectionAddRequest;
import com.hazelcast.collection.client.CollectionClearRequest;
import com.hazelcast.collection.client.CollectionCompareAndRemoveRequest;
import com.hazelcast.collection.client.CollectionContainsRequest;
import com.hazelcast.collection.client.CollectionGetAllRequest;
import com.hazelcast.collection.client.CollectionIsEmptyRequest;
import com.hazelcast.collection.client.CollectionRemoveListenerRequest;
import com.hazelcast.collection.client.CollectionRemoveRequest;
import com.hazelcast.collection.client.CollectionSizeRequest;
import com.hazelcast.collection.client.ListAddAllRequest;
import com.hazelcast.collection.client.ListAddRequest;
import com.hazelcast.collection.client.ListGetRequest;
import com.hazelcast.collection.client.ListIndexOfRequest;
import com.hazelcast.collection.client.ListRemoveRequest;
import com.hazelcast.collection.client.ListSetRequest;
import com.hazelcast.collection.client.ListSubRequest;
import com.hazelcast.collection.client.TxnListAddRequest;
import com.hazelcast.collection.client.TxnListRemoveRequest;
import com.hazelcast.collection.client.TxnListSizeRequest;
import com.hazelcast.collection.client.TxnSetAddRequest;
import com.hazelcast.collection.client.TxnSetRemoveRequest;
import com.hazelcast.collection.client.TxnSetSizeRequest;
import com.hazelcast.nio.serialization.ArrayPortableFactory;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;

public class CollectionPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.COLLECTION_PORTABLE_FACTORY, -20);

    public static final int COLLECTION_SIZE = 1;
    public static final int COLLECTION_CONTAINS = 2;
    public static final int COLLECTION_ADD = 3;
    public static final int COLLECTION_REMOVE = 4;
    public static final int COLLECTION_ADD_ALL = 5;
    public static final int COLLECTION_COMPARE_AND_REMOVE = 6;
    public static final int COLLECTION_CLEAR = 7;
    public static final int COLLECTION_GET_ALL = 8;
    public static final int COLLECTION_ADD_LISTENER = 9;
    public static final int LIST_ADD_ALL = 10;
    public static final int LIST_GET = 11;
    public static final int LIST_SET = 12;
    public static final int LIST_ADD = 13;
    public static final int LIST_REMOVE = 14;
    public static final int LIST_INDEX_OF = 15;
    public static final int LIST_SUB = 16;

    public static final int TXN_LIST_ADD = 17;
    public static final int TXN_LIST_REMOVE = 18;
    public static final int TXN_LIST_SIZE = 19;

    public static final int TXN_SET_ADD = 20;
    public static final int TXN_SET_REMOVE = 21;
    public static final int TXN_SET_SIZE = 22;
    public static final int COLLECTION_REMOVE_LISTENER = 23;
    public static final int COLLECTION_IS_EMPTY = 24;

    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public PortableFactory createFactory() {
        ConstructorFunction<Integer, Portable>[] constructors = new ConstructorFunction[COLLECTION_IS_EMPTY + 1];

        constructors[COLLECTION_SIZE] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new CollectionSizeRequest();
            }
        };
        constructors[COLLECTION_CONTAINS] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new CollectionContainsRequest();
            }
        };
        constructors[COLLECTION_ADD] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new CollectionAddRequest();
            }
        };
        constructors[COLLECTION_REMOVE] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new CollectionRemoveRequest();
            }
        };
        constructors[COLLECTION_ADD_ALL] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new CollectionAddAllRequest();
            }
        };
        constructors[COLLECTION_COMPARE_AND_REMOVE] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new CollectionCompareAndRemoveRequest();
            }
        };
        constructors[COLLECTION_CLEAR] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new CollectionClearRequest();
            }
        };
        constructors[COLLECTION_GET_ALL] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new CollectionGetAllRequest();
            }
        };
        constructors[COLLECTION_ADD_LISTENER] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new CollectionAddListenerRequest();
            }
        };

        constructors[LIST_ADD_ALL] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new ListAddAllRequest();
            }
        };
        constructors[LIST_GET] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new ListGetRequest();
            }
        };
        constructors[LIST_SET] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new ListSetRequest();
            }
        };
        constructors[LIST_ADD] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new ListAddRequest();
            }
        };
        constructors[LIST_REMOVE] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new ListRemoveRequest();
            }
        };
        constructors[LIST_INDEX_OF] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new ListIndexOfRequest();
            }
        };
        constructors[LIST_SUB] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new ListSubRequest();
            }
        };


        constructors[TXN_LIST_ADD] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new TxnListAddRequest();
            }
        };
        constructors[TXN_LIST_REMOVE] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new TxnListRemoveRequest();
            }
        };
        constructors[TXN_LIST_SIZE] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new TxnListSizeRequest();
            }
        };
        constructors[TXN_SET_ADD] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new TxnSetAddRequest();
            }
        };
        constructors[TXN_SET_REMOVE] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new TxnSetRemoveRequest();
            }
        };
        constructors[TXN_SET_SIZE] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new TxnSetSizeRequest();
            }
        };
        constructors[COLLECTION_REMOVE_LISTENER] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new CollectionRemoveListenerRequest();
            }
        };
        constructors[COLLECTION_IS_EMPTY] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new CollectionIsEmptyRequest();
            }
        };

        return new ArrayPortableFactory(constructors);
    }

    @Override
    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
