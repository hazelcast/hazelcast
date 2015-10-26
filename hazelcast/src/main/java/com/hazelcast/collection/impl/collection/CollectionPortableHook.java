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

package com.hazelcast.collection.impl.collection;

import com.hazelcast.collection.impl.collection.client.CollectionAddAllRequest;
import com.hazelcast.collection.impl.collection.client.CollectionAddListenerRequest;
import com.hazelcast.collection.impl.collection.client.CollectionAddRequest;
import com.hazelcast.collection.impl.collection.client.CollectionClearRequest;
import com.hazelcast.collection.impl.collection.client.CollectionCompareAndRemoveRequest;
import com.hazelcast.collection.impl.collection.client.CollectionContainsRequest;
import com.hazelcast.collection.impl.collection.client.CollectionGetAllRequest;
import com.hazelcast.collection.impl.collection.client.CollectionIsEmptyRequest;
import com.hazelcast.collection.impl.collection.client.CollectionRemoveListenerRequest;
import com.hazelcast.collection.impl.collection.client.CollectionRemoveRequest;
import com.hazelcast.collection.impl.collection.client.CollectionSizeRequest;
import com.hazelcast.collection.impl.list.client.ListAddAllRequest;
import com.hazelcast.collection.impl.list.client.ListAddRequest;
import com.hazelcast.collection.impl.list.client.ListGetRequest;
import com.hazelcast.collection.impl.list.client.ListIndexOfRequest;
import com.hazelcast.collection.impl.list.client.ListRemoveRequest;
import com.hazelcast.collection.impl.list.client.ListSetRequest;
import com.hazelcast.collection.impl.list.client.ListSubRequest;
import com.hazelcast.collection.impl.txnlist.client.TxnListAddRequest;
import com.hazelcast.collection.impl.txnlist.client.TxnListRemoveRequest;
import com.hazelcast.collection.impl.txnlist.client.TxnListSizeRequest;
import com.hazelcast.collection.impl.txnset.client.TxnSetAddRequest;
import com.hazelcast.collection.impl.txnset.client.TxnSetRemoveRequest;
import com.hazelcast.collection.impl.txnset.client.TxnSetSizeRequest;
import com.hazelcast.internal.serialization.PortableHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

import java.util.Collection;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.COLLECTION_PORTABLE_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.COLLECTION_PORTABLE_FACTORY_ID;

public class CollectionPortableHook implements PortableHook {
    public static final int F_ID = FactoryIdHelper.getFactoryId(COLLECTION_PORTABLE_FACTORY, COLLECTION_PORTABLE_FACTORY_ID);
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

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    public PortableFactory createFactory() {
        return new PortableFactory() {
            @Override
            public Portable create(int classId) {
                switch (classId) {
                    case COLLECTION_SIZE:
                        return new CollectionSizeRequest();
                    case COLLECTION_CONTAINS:
                        return new CollectionContainsRequest();
                    case COLLECTION_ADD:
                        return new CollectionAddRequest();
                    case COLLECTION_REMOVE:
                        return new CollectionRemoveRequest();
                    case COLLECTION_ADD_ALL:
                        return new CollectionAddAllRequest();
                    case COLLECTION_COMPARE_AND_REMOVE:
                        return new CollectionCompareAndRemoveRequest();
                    case COLLECTION_CLEAR:
                        return new CollectionClearRequest();
                    case COLLECTION_GET_ALL:
                        return new CollectionGetAllRequest();
                    case COLLECTION_ADD_LISTENER:
                        return new CollectionAddListenerRequest();
                    case LIST_ADD_ALL:
                        return new ListAddAllRequest();
                    case LIST_GET:
                        return new ListGetRequest();
                    case LIST_SET:
                        return new ListSetRequest();
                    case LIST_ADD:
                        return new ListAddRequest();
                    case LIST_REMOVE:
                        return new ListRemoveRequest();
                    case LIST_INDEX_OF:
                        return new ListIndexOfRequest();
                    case LIST_SUB:
                        return new ListSubRequest();
                    case TXN_LIST_ADD:
                        return new TxnListAddRequest();
                    case TXN_LIST_REMOVE:
                        return new TxnListRemoveRequest();
                    case TXN_LIST_SIZE:
                        return new TxnListSizeRequest();
                    case TXN_SET_ADD:
                        return new TxnSetAddRequest();
                    case TXN_SET_REMOVE:
                        return new TxnSetRemoveRequest();
                    case TXN_SET_SIZE:
                        return new TxnSetSizeRequest();
                    case COLLECTION_REMOVE_LISTENER:
                        return new CollectionRemoveListenerRequest();
                    case COLLECTION_IS_EMPTY:
                        return new CollectionIsEmptyRequest();
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
