/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.collection.client.*;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;

/**
 * @ali 9/4/13
 */
public class CollectionPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.COLLECTION_PORTABLE_FACTORY, -20);

    public static int increment = 1;

    public static final int COLLECTION_SIZE = increment++;
    public static final int COLLECTION_CONTAINS = increment++;
    public static final int COLLECTION_ADD = increment++;
    public static final int COLLECTION_REMOVE = increment++;
    public static final int COLLECTION_ADD_ALL = increment++;
    public static final int COLLECTION_COMPARE_AND_REMOVE = increment++;
    public static final int COLLECTION_CLEAR = increment++;
    public static final int COLLECTION_GET_ALL = increment++;
    public static final int COLLECTION_ADD_LISTENER = increment++;
    public static final int COLLECTION_DESTROY = increment++;
    public static final int LIST_ADD_ALL = increment++;
    public static final int LIST_GET = increment++;
    public static final int LIST_SET = increment++;
    public static final int LIST_ADD = increment++;
    public static final int LIST_REMOVE = increment++;
    public static final int LIST_INDEX_OF = increment++;
    public static final int LIST_SUB = increment++;

    public static final int TXN_LIST_ADD = increment++;
    public static final int TXN_LIST_REMOVE = increment++;
    public static final int TXN_LIST_SIZE = increment++;

    public static final int TXN_SET_ADD = increment++;
    public static final int TXN_SET_REMOVE = increment++;
    public static final int TXN_SET_SIZE = increment++;

    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public PortableFactory createFactory() {
        ConstructorFunction<Integer, Portable> constructors[] = new ConstructorFunction[increment];

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
        constructors[COLLECTION_DESTROY] = new ConstructorFunction<Integer, Portable>() {
            public Portable createNew(Integer arg) {
                return new CollectionDestroyRequest();
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

        return new ArrayPortableFactory(constructors);
    }

    @Override
    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
