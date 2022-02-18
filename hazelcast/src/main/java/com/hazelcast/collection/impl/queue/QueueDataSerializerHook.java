/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.collection.impl.queue.operations.AddAllBackupOperation;
import com.hazelcast.collection.impl.queue.operations.AddAllOperation;
import com.hazelcast.collection.impl.queue.operations.CheckAndEvictOperation;
import com.hazelcast.collection.impl.queue.operations.ClearBackupOperation;
import com.hazelcast.collection.impl.queue.operations.ClearOperation;
import com.hazelcast.collection.impl.queue.operations.CompareAndRemoveBackupOperation;
import com.hazelcast.collection.impl.queue.operations.CompareAndRemoveOperation;
import com.hazelcast.collection.impl.queue.operations.ContainsOperation;
import com.hazelcast.collection.impl.queue.operations.DrainBackupOperation;
import com.hazelcast.collection.impl.queue.operations.DrainOperation;
import com.hazelcast.collection.impl.queue.operations.IsEmptyOperation;
import com.hazelcast.collection.impl.queue.operations.IteratorOperation;
import com.hazelcast.collection.impl.queue.operations.OfferBackupOperation;
import com.hazelcast.collection.impl.queue.operations.OfferOperation;
import com.hazelcast.collection.impl.queue.operations.PeekOperation;
import com.hazelcast.collection.impl.queue.operations.PollBackupOperation;
import com.hazelcast.collection.impl.queue.operations.PollOperation;
import com.hazelcast.collection.impl.queue.operations.QueueMergeBackupOperation;
import com.hazelcast.collection.impl.queue.operations.QueueMergeOperation;
import com.hazelcast.collection.impl.queue.operations.QueueReplicationOperation;
import com.hazelcast.collection.impl.queue.operations.RemainingCapacityOperation;
import com.hazelcast.collection.impl.queue.operations.RemoveBackupOperation;
import com.hazelcast.collection.impl.queue.operations.RemoveOperation;
import com.hazelcast.collection.impl.queue.operations.SizeOperation;
import com.hazelcast.collection.impl.txnqueue.TxQueueItem;
import com.hazelcast.collection.impl.txnqueue.operations.QueueTransactionRollbackOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnCommitBackupOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnCommitOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnOfferBackupOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnOfferOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnPeekOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnPollBackupOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnPollOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnPrepareBackupOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnPrepareOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnReserveOfferBackupOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnReserveOfferOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnReservePollBackupOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnReservePollOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnRollbackBackupOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnRollbackOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.QUEUE_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.QUEUE_DS_FACTORY_ID;

/**
 * A {@link DataSerializerHook} for the queue operations and support structures.
 */
public final class QueueDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(QUEUE_DS_FACTORY, QUEUE_DS_FACTORY_ID);

    public static final int OFFER = 0;
    public static final int POLL = 1;
    public static final int PEEK = 2;

    public static final int OFFER_BACKUP = 3;
    public static final int POLL_BACKUP = 4;

    public static final int ADD_ALL_BACKUP = 5;
    public static final int ADD_ALL = 6;
    public static final int CLEAR_BACKUP = 7;
    public static final int CLEAR = 8;
    public static final int COMPARE_AND_REMOVE_BACKUP = 9;
    public static final int COMPARE_AND_REMOVE = 10;
    public static final int CONTAINS = 11;
    public static final int DRAIN_BACKUP = 12;
    public static final int DRAIN = 13;
    public static final int ITERATOR = 14;
    public static final int QUEUE_EVENT = 15;
    public static final int QUEUE_EVENT_FILTER = 16;
    public static final int QUEUE_ITEM = 17;
    public static final int QUEUE_REPLICATION = 18;
    public static final int REMOVE_BACKUP = 19;
    public static final int REMOVE = 20;
    //    static final int EMPTY_ID = 21;
    public static final int SIZE = 22;

    public static final int TXN_OFFER_BACKUP = 23;
    public static final int TXN_OFFER = 24;
    public static final int TXN_POLL_BACKUP = 25;
    public static final int TXN_POLL = 26;
    public static final int TXN_PREPARE_BACKUP = 27;
    public static final int TXN_PREPARE = 28;
    public static final int TXN_RESERVE_OFFER = 29;
    public static final int TXN_RESERVE_OFFER_BACKUP = 30;
    public static final int TXN_RESERVE_POLL = 31;
    public static final int TXN_RESERVE_POLL_BACKUP = 32;
    public static final int TXN_ROLLBACK_BACKUP = 33;

    public static final int TXN_ROLLBACK = 34;
    public static final int CHECK_EVICT = 35;
    public static final int TRANSACTION_ROLLBACK = 36;
    public static final int TX_QUEUE_ITEM = 37;
    public static final int QUEUE_CONTAINER = 38;
    public static final int TXN_PEEK = 39;
    public static final int IS_EMPTY = 40;
    public static final int REMAINING_CAPACITY = 41;

    public static final int TXN_COMMIT = 42;
    public static final int TXN_COMMIT_BACKUP = 43;

    public static final int MERGE = 44;
    public static final int MERGE_BACKUP = 45;

    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {

        //noinspection unchecked
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[MERGE_BACKUP + 1];
        constructors[OFFER] = arg -> new OfferOperation();
        constructors[OFFER_BACKUP] = arg -> new OfferBackupOperation();
        constructors[POLL] = arg -> new PollOperation();
        constructors[POLL_BACKUP] = arg -> new PollBackupOperation();
        constructors[PEEK] = arg -> new PeekOperation();
        constructors[ADD_ALL_BACKUP] = arg -> new AddAllBackupOperation();
        constructors[ADD_ALL] = arg -> new AddAllOperation();
        constructors[CLEAR_BACKUP] = arg -> new ClearBackupOperation();
        constructors[CLEAR] = arg -> new ClearOperation();
        constructors[COMPARE_AND_REMOVE_BACKUP] = arg -> new CompareAndRemoveBackupOperation();
        constructors[COMPARE_AND_REMOVE] = arg -> new CompareAndRemoveOperation();
        constructors[CONTAINS] = arg -> new ContainsOperation();
        constructors[DRAIN_BACKUP] = arg -> new DrainBackupOperation();
        constructors[DRAIN] = arg -> new DrainOperation();
        constructors[ITERATOR] = arg -> new IteratorOperation();
        constructors[QUEUE_EVENT] = arg -> new QueueEvent();
        constructors[QUEUE_EVENT_FILTER] = arg -> new QueueEventFilter();
        constructors[QUEUE_ITEM] = arg -> new QueueItem();
        constructors[QUEUE_REPLICATION] = arg -> new QueueReplicationOperation();
        constructors[REMOVE_BACKUP] = arg -> new RemoveBackupOperation();
        constructors[REMOVE] = arg -> new RemoveOperation();
        constructors[SIZE] = arg -> new SizeOperation();
        constructors[TXN_OFFER_BACKUP] = arg -> new TxnOfferBackupOperation();
        constructors[TXN_OFFER] = arg -> new TxnOfferOperation();
        constructors[TXN_POLL_BACKUP] = arg -> new TxnPollBackupOperation();
        constructors[TXN_POLL] = arg -> new TxnPollOperation();
        constructors[TXN_PREPARE_BACKUP] = arg -> new TxnPrepareBackupOperation();
        constructors[TXN_PREPARE] = arg -> new TxnPrepareOperation();
        constructors[TXN_RESERVE_OFFER] = arg -> new TxnReserveOfferOperation();
        constructors[TXN_RESERVE_OFFER_BACKUP] = arg -> new TxnReserveOfferBackupOperation();
        constructors[TXN_RESERVE_POLL] = arg -> new TxnReservePollOperation();
        constructors[TXN_RESERVE_POLL_BACKUP] = arg -> new TxnReservePollBackupOperation();
        constructors[TXN_ROLLBACK_BACKUP] = arg -> new TxnRollbackBackupOperation();
        constructors[TXN_ROLLBACK] = arg -> new TxnRollbackOperation();
        constructors[CHECK_EVICT] = arg -> new CheckAndEvictOperation();
        constructors[QUEUE_CONTAINER] = arg -> new QueueContainer();
        constructors[TRANSACTION_ROLLBACK] = arg -> new QueueTransactionRollbackOperation();
        constructors[TX_QUEUE_ITEM] = arg -> new TxQueueItem();
        constructors[TXN_PEEK] = arg -> new TxnPeekOperation();
        constructors[IS_EMPTY] = arg -> new IsEmptyOperation();
        constructors[REMAINING_CAPACITY] = arg -> new RemainingCapacityOperation();
        constructors[TXN_COMMIT] = arg -> new TxnCommitOperation();
        constructors[TXN_COMMIT_BACKUP] = arg -> new TxnCommitBackupOperation();
        constructors[MERGE] = arg -> new QueueMergeOperation();
        constructors[MERGE_BACKUP] = arg -> new QueueMergeBackupOperation();

        return new ArrayDataSerializableFactory(constructors);
    }
}
