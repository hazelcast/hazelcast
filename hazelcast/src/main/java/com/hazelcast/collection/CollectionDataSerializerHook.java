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

import com.hazelcast.collection.list.*;
import com.hazelcast.collection.set.SetReplicationOperation;
import com.hazelcast.collection.txn.*;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.util.ConstructorFunction;

/**
 * @ali 8/30/13
 */
public class CollectionDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdRepository.getDSFactoryId(FactoryIdRepository.COLLECTION);

    private static int increment = 1;

    public static final int COLLECTION_ADD = increment++;
    public static final int COLLECTION_ADD_BACKUP = increment++;
    public static final int LIST_ADD = increment++;
    public static final int LIST_GET = increment++;
    public static final int COLLECTION_REMOVE = increment++;
    public static final int COLLECTION_REMOVE_BACKUP = increment++;
    public static final int COLLECTION_SIZE = increment++;
    public static final int COLLECTION_CLEAR = increment++;
    public static final int COLLECTION_CLEAR_BACKUP = increment++;
    public static final int LIST_SET = increment++;
    public static final int LIST_SET_BACKUP = increment++;
    public static final int LIST_REMOVE = increment++;
    public static final int LIST_INDEX_OF = increment++;
    public static final int COLLECTION_CONTAINS = increment++;
    public static final int COLLECTION_ADD_ALL = increment++;
    public static final int COLLECTION_ADD_ALL_BACKUP = increment++;
    public static final int LIST_ADD_ALL = increment++;
    public static final int LIST_SUB = increment++;
    public static final int COLLECTION_COMPARE_AND_REMOVE = increment++;
    public static final int COLLECTION_GET_ALL = increment++;
    public static final int COLLECTION_EVENT_FILTER = increment++;
    public static final int COLLECTION_EVENT = increment++;
    public static final int COLLECTION_ITEM = increment++;

    public static final int COLLECTION_RESERVE_ADD = increment++;
    public static final int COLLECTION_RESERVE_REMOVE = increment++;
    public static final int COLLECTION_TXN_ADD = increment++;
    public static final int COLLECTION_TXN_ADD_BACKUP = increment++;
    public static final int COLLECTION_TXN_REMOVE = increment++;
    public static final int COLLECTION_TXN_REMOVE_BACKUP = increment++;

    public static final int COLLECTION_PREPARE = increment++;
    public static final int COLLECTION_PREPARE_BACKUP = increment++;
    public static final int COLLECTION_ROLLBACK = increment++;
    public static final int COLLECTION_ROLLBACK_BACKUP = increment++;

    public static final int TX_COLLECTION_ITEM = increment++;
    public static final int TX_ROLLBACK = increment++;

    public static final int LIST_REPLICATION = increment++;
    public static final int SET_REPLICATION = increment++;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable> constructors[] = new ConstructorFunction[increment];

        constructors[COLLECTION_ADD] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionAddOperation();
            }
        };
        constructors[COLLECTION_ADD_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionAddBackupOperation();
            }
        };
        constructors[LIST_ADD] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ListAddOperation();
            }
        };
        constructors[LIST_GET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ListGetOperation();
            }
        };
        constructors[COLLECTION_REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionRemoveOperation();
            }
        };
        constructors[COLLECTION_REMOVE_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionRemoveBackupOperation();
            }
        };
        constructors[COLLECTION_SIZE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionSizeOperation();
            }
        };
        constructors[COLLECTION_CLEAR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionClearOperation();
            }
        };
        constructors[COLLECTION_CLEAR_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionClearBackupOperation();
            }
        };
        constructors[LIST_SET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ListSetOperation();
            }
        };
        constructors[LIST_SET_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ListSetBackupOperation();
            }
        };
        constructors[LIST_REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ListRemoveOperation();
            }
        };
        constructors[LIST_INDEX_OF] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ListIndexOfOperation();
            }
        };
        constructors[COLLECTION_CONTAINS] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionContainsOperation();
            }
        };
        constructors[COLLECTION_ADD_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionAddAllOperation();
            }
        };
        constructors[COLLECTION_ADD_ALL_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionAddAllBackupOperation();
            }
        };
        constructors[LIST_ADD_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ListAddAllOperation();
            }
        };
        constructors[LIST_SUB] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ListSubOperation();
            }
        };
        constructors[COLLECTION_COMPARE_AND_REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionCompareAndRemoveOperation();
            }
        };
        constructors[COLLECTION_GET_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionGetAllOperation();
            }
        };
        constructors[COLLECTION_EVENT_FILTER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionEventFilter();
            }
        };
        constructors[COLLECTION_EVENT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionEvent();
            }
        };
        constructors[COLLECTION_ITEM] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionItem();
            }
        };



        constructors[COLLECTION_RESERVE_ADD] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionReserveAddOperation();
            }
        };
        constructors[COLLECTION_RESERVE_REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionReserveRemoveOperation();
            }
        };
        constructors[COLLECTION_TXN_ADD] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionTxnAddOperation();
            }
        };
        constructors[COLLECTION_TXN_ADD_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionTxnAddBackupOperation();
            }
        };
        constructors[COLLECTION_TXN_REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionTxnRemoveOperation();
            }
        };
        constructors[COLLECTION_TXN_REMOVE_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionTxnRemoveBackupOperation();
            }
        };


        constructors[COLLECTION_PREPARE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionPrepareOperation();
            }
        };
        constructors[COLLECTION_PREPARE_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionPrepareBackupOperation();
            }
        };
        constructors[COLLECTION_ROLLBACK] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionRollbackOperation();
            }
        };
        constructors[COLLECTION_ROLLBACK_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionRollbackBackupOperation();
            }
        };
        constructors[TX_COLLECTION_ITEM] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxCollectionItem();
            }
        };
        constructors[TX_ROLLBACK] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionTransactionRollbackOperation();
            }
        };
        constructors[LIST_REPLICATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ListReplicationOperation();
            }
        };
        constructors[SET_REPLICATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SetReplicationOperation();
            }
        };

        return new ArrayDataSerializableFactory(constructors);
    }
}
