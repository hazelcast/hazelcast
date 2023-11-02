/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.collection.impl.collection.operations.CollectionAddAllBackupOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionAddAllOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionAddBackupOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionAddOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionClearBackupOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionClearOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionCompareAndRemoveOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionContainsOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionGetAllOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionIsEmptyOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionMergeBackupOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionMergeOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionRemoveBackupOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionRemoveOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionSizeOperation;
import com.hazelcast.collection.impl.list.ListContainer;
import com.hazelcast.collection.impl.list.operations.ListAddAllOperation;
import com.hazelcast.collection.impl.list.operations.ListAddOperation;
import com.hazelcast.collection.impl.list.operations.ListGetOperation;
import com.hazelcast.collection.impl.list.operations.ListIndexOfOperation;
import com.hazelcast.collection.impl.list.operations.ListRemoveOperation;
import com.hazelcast.collection.impl.list.operations.ListReplicationOperation;
import com.hazelcast.collection.impl.list.operations.ListSetBackupOperation;
import com.hazelcast.collection.impl.list.operations.ListSetOperation;
import com.hazelcast.collection.impl.list.operations.ListSubOperation;
import com.hazelcast.collection.impl.set.SetContainer;
import com.hazelcast.collection.impl.set.operations.SetReplicationOperation;
import com.hazelcast.collection.impl.txncollection.CollectionTransactionLogRecord;
import com.hazelcast.collection.impl.txncollection.operations.CollectionCommitBackupOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionCommitOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionPrepareBackupOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionPrepareOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionReserveAddOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionReserveRemoveOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionRollbackBackupOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionRollbackOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionTransactionRollbackOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionTxnAddBackupOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionTxnAddOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionTxnRemoveBackupOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionTxnRemoveOperation;
import com.hazelcast.collection.impl.txnqueue.QueueTransactionLogRecord;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.function.Supplier;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.COLLECTION_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.COLLECTION_DS_FACTORY_ID;

public class CollectionDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(COLLECTION_DS_FACTORY, COLLECTION_DS_FACTORY_ID);

    public static final int COLLECTION_ADD = 1;
    public static final int COLLECTION_ADD_BACKUP = 2;
    public static final int LIST_ADD = 3;
    public static final int LIST_GET = 4;
    public static final int COLLECTION_REMOVE = 5;
    public static final int COLLECTION_REMOVE_BACKUP = 6;
    public static final int COLLECTION_SIZE = 7;
    public static final int COLLECTION_CLEAR = 8;
    public static final int COLLECTION_CLEAR_BACKUP = 9;
    public static final int LIST_SET = 10;
    public static final int LIST_SET_BACKUP = 11;
    public static final int LIST_REMOVE = 12;
    public static final int LIST_INDEX_OF = 13;
    public static final int COLLECTION_CONTAINS = 14;
    public static final int COLLECTION_ADD_ALL = 15;
    public static final int COLLECTION_ADD_ALL_BACKUP = 16;
    public static final int LIST_ADD_ALL = 17;
    public static final int LIST_SUB = 18;
    public static final int COLLECTION_COMPARE_AND_REMOVE = 19;
    public static final int COLLECTION_GET_ALL = 20;
    public static final int COLLECTION_EVENT_FILTER = 21;
    public static final int COLLECTION_EVENT = 22;
    public static final int COLLECTION_ITEM = 23;

    public static final int COLLECTION_RESERVE_ADD = 24;
    public static final int COLLECTION_RESERVE_REMOVE = 25;
    public static final int COLLECTION_TXN_ADD = 26;
    public static final int COLLECTION_TXN_ADD_BACKUP = 27;
    public static final int COLLECTION_TXN_REMOVE = 28;
    public static final int COLLECTION_TXN_REMOVE_BACKUP = 29;

    public static final int COLLECTION_PREPARE = 30;
    public static final int COLLECTION_PREPARE_BACKUP = 31;
    public static final int COLLECTION_ROLLBACK = 32;
    public static final int COLLECTION_ROLLBACK_BACKUP = 33;

    public static final int TX_COLLECTION_ITEM = 34;
    public static final int TX_ROLLBACK = 35;

    public static final int LIST_REPLICATION = 36;
    public static final int SET_REPLICATION = 37;

    public static final int COLLECTION_IS_EMPTY = 38;

    public static final int TXN_COMMIT = 39;
    public static final int TXN_COMMIT_BACKUP = 40;

    public static final int SET_CONTAINER = 41;
    public static final int LIST_CONTAINER = 42;
    public static final int COLLECTION_TRANSACTION_LOG_RECORD = 43;
    public static final int QUEUE_TRANSACTION_LOG_RECORD = 44;

    public static final int COLLECTION_MERGE = 45;
    public static final int COLLECTION_MERGE_BACKUP = 46;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        Supplier<IdentifiedDataSerializable>[] constructors = new Supplier[COLLECTION_MERGE_BACKUP + 1];

        constructors[COLLECTION_ADD] = CollectionAddOperation::new;
        constructors[COLLECTION_ADD_BACKUP] = CollectionAddBackupOperation::new;
        constructors[LIST_ADD] = ListAddOperation::new;
        constructors[LIST_GET] = ListGetOperation::new;
        constructors[COLLECTION_REMOVE] = CollectionRemoveOperation::new;
        constructors[COLLECTION_REMOVE_BACKUP] = CollectionRemoveBackupOperation::new;
        constructors[COLLECTION_SIZE] = CollectionSizeOperation::new;
        constructors[COLLECTION_CLEAR] = CollectionClearOperation::new;
        constructors[COLLECTION_CLEAR_BACKUP] = CollectionClearBackupOperation::new;
        constructors[LIST_SET] = ListSetOperation::new;
        constructors[LIST_SET_BACKUP] = ListSetBackupOperation::new;
        constructors[LIST_REMOVE] = ListRemoveOperation::new;
        constructors[LIST_INDEX_OF] = ListIndexOfOperation::new;
        constructors[COLLECTION_CONTAINS] = CollectionContainsOperation::new;
        constructors[COLLECTION_ADD_ALL] = CollectionAddAllOperation::new;
        constructors[COLLECTION_ADD_ALL_BACKUP] = CollectionAddAllBackupOperation::new;
        constructors[LIST_ADD_ALL] = ListAddAllOperation::new;
        constructors[LIST_SUB] = ListSubOperation::new;
        constructors[COLLECTION_COMPARE_AND_REMOVE] = CollectionCompareAndRemoveOperation::new;
        constructors[COLLECTION_GET_ALL] = CollectionGetAllOperation::new;
        constructors[COLLECTION_EVENT_FILTER] = CollectionEventFilter::new;
        constructors[COLLECTION_EVENT] = CollectionEvent::new;
        constructors[COLLECTION_ITEM] = CollectionItem::new;

        constructors[COLLECTION_RESERVE_ADD] = CollectionReserveAddOperation::new;
        constructors[COLLECTION_RESERVE_REMOVE] = CollectionReserveRemoveOperation::new;
        constructors[COLLECTION_TXN_ADD] = CollectionTxnAddOperation::new;
        constructors[COLLECTION_TXN_ADD_BACKUP] = CollectionTxnAddBackupOperation::new;
        constructors[COLLECTION_TXN_REMOVE] = CollectionTxnRemoveOperation::new;
        constructors[COLLECTION_TXN_REMOVE_BACKUP] = CollectionTxnRemoveBackupOperation::new;
        constructors[COLLECTION_PREPARE] = CollectionPrepareOperation::new;
        constructors[COLLECTION_PREPARE_BACKUP] = CollectionPrepareBackupOperation::new;
        constructors[COLLECTION_ROLLBACK] = CollectionRollbackOperation::new;
        constructors[COLLECTION_ROLLBACK_BACKUP] = CollectionRollbackBackupOperation::new;
        constructors[TX_COLLECTION_ITEM] = TxCollectionItem::new;
        constructors[TX_ROLLBACK] = CollectionTransactionRollbackOperation::new;
        constructors[LIST_REPLICATION] = ListReplicationOperation::new;
        constructors[SET_REPLICATION] = SetReplicationOperation::new;
        constructors[COLLECTION_IS_EMPTY] = CollectionIsEmptyOperation::new;
        constructors[TXN_COMMIT] = CollectionCommitOperation::new;
        constructors[TXN_COMMIT_BACKUP] = CollectionCommitBackupOperation::new;
        constructors[SET_CONTAINER] = SetContainer::new;
        constructors[LIST_CONTAINER] = ListContainer::new;
        constructors[COLLECTION_TRANSACTION_LOG_RECORD] = CollectionTransactionLogRecord::new;
        constructors[QUEUE_TRANSACTION_LOG_RECORD] = QueueTransactionLogRecord::new;
        constructors[COLLECTION_MERGE] = CollectionMergeOperation::new;
        constructors[COLLECTION_MERGE_BACKUP] = CollectionMergeBackupOperation::new;

        return new ArrayDataSerializableFactory(constructors);
    }
}
