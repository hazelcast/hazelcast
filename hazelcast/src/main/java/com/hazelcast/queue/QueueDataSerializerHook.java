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

package com.hazelcast.queue;

import com.hazelcast.nio.serialization.ArrayDataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.queue.tx.QueueTransactionRollbackOperation;
import com.hazelcast.queue.tx.TxnOfferBackupOperation;
import com.hazelcast.queue.tx.TxnOfferOperation;
import com.hazelcast.queue.tx.TxnPeekOperation;
import com.hazelcast.queue.tx.TxnPollBackupOperation;
import com.hazelcast.queue.tx.TxnPollOperation;
import com.hazelcast.queue.tx.TxnPrepareBackupOperation;
import com.hazelcast.queue.tx.TxnPrepareOperation;
import com.hazelcast.queue.tx.TxnReserveOfferOperation;
import com.hazelcast.queue.tx.TxnReservePollOperation;
import com.hazelcast.queue.tx.TxnRollbackBackupOperation;
import com.hazelcast.queue.tx.TxnRollbackOperation;
import com.hazelcast.util.ConstructorFunction;

/**
 * @author mdogan 8/24/12
 */
public final class QueueDataSerializerHook implements DataSerializerHook {

    static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.QUEUE_DS_FACTORY, -11);

    static final int OFFER = 0;
    static final int POLL = 1;
    static final int PEEK = 2;

    static final int OFFER_BACKUP = 3;
    static final int POLL_BACKUP = 4;

    static final int ADD_ALL_BACKUP = 5;
    static final int ADD_ALL = 6;
    static final int CLEAR_BACKUP = 7;
    static final int CLEAR = 8;
    static final int COMPARE_AND_REMOVE_BACKUP = 9;
    static final int COMPARE_AND_REMOVE = 10;
    static final int CONTAINS = 11;
    static final int DRAIN_BACKUP = 12;
    static final int DRAIN = 13;
    static final int ITERATOR = 14;
    static final int QUEUE_EVENT = 15;
    static final int QUEUE_EVENT_FILTER = 16;
    static final int QUEUE_ITEM = 17;
    static final int QUEUE_REPLICATION = 18;
    static final int REMOVE_BACKUP = 19;
    static final int REMOVE = 20;
    //    static final int EMPTY_ID = 21;
    static final int SIZE = 22;

    public static final int TXN_OFFER_BACKUP = 23;
    public static final int TXN_OFFER = 24;
    public static final int TXN_POLL_BACKUP = 25;
    public static final int TXN_POLL = 26;
    public static final int TXN_PREPARE_BACKUP = 27;
    public static final int TXN_PREPARE = 28;
    public static final int TXN_RESERVE_OFFER = 29;
    public static final int TXN_RESERVE_POLL = 30;
    public static final int TXN_ROLLBACK_BACKUP = 31;
    public static final int TXN_ROLLBACK = 32;

    public static final int CHECK_EVICT = 33;
    public static final int TRANSACTION_ROLLBACK = 34;
    public static final int TX_QUEUE_ITEM = 35;
    public static final int QUEUE_CONTAINER = 36;
    public static final int TXN_PEEK = 37;


    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {

        ConstructorFunction<Integer, IdentifiedDataSerializable> constructors[] = new ConstructorFunction[TXN_PEEK + 1];
        constructors[OFFER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new OfferOperation();
            }
        };

        constructors[OFFER_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new OfferBackupOperation();
            }
        };
        constructors[POLL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PollOperation();
            }
        };
        constructors[POLL_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PollBackupOperation();
            }
        };
        constructors[PEEK] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PeekOperation();
            }
        };
        constructors[ADD_ALL_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AddAllBackupOperation();
            }
        };
        constructors[ADD_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AddAllOperation();
            }
        };
        constructors[CLEAR_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClearBackupOperation();
            }
        };
        constructors[CLEAR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClearOperation();
            }
        };
        constructors[COMPARE_AND_REMOVE_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CompareAndRemoveBackupOperation();
            }
        };
        constructors[COMPARE_AND_REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CompareAndRemoveOperation();
            }
        };
        constructors[CONTAINS] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ContainsOperation();
            }
        };
        constructors[DRAIN_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DrainBackupOperation();
            }
        };
        constructors[DRAIN] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DrainOperation();
            }
        };
        constructors[ITERATOR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new IteratorOperation();
            }
        };
        constructors[QUEUE_EVENT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new QueueEvent();
            }
        };
        constructors[QUEUE_EVENT_FILTER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new QueueEventFilter();
            }
        };
        constructors[QUEUE_ITEM] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new QueueItem();
            }
        };
        constructors[QUEUE_REPLICATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new QueueReplicationOperation();
            }
        };
        constructors[REMOVE_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RemoveBackupOperation();
            }
        };
        constructors[REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RemoveOperation();
            }
        };
        constructors[SIZE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SizeOperation();
            }
        };
        constructors[TXN_OFFER_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnOfferBackupOperation();
            }
        };
        constructors[TXN_OFFER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnOfferOperation();
            }
        };
        constructors[TXN_POLL_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnPollBackupOperation();
            }
        };
        constructors[TXN_POLL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnPollOperation();
            }
        };
        constructors[TXN_PREPARE_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnPrepareBackupOperation();
            }
        };
        constructors[TXN_PREPARE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnPrepareOperation();
            }
        };
        constructors[TXN_RESERVE_OFFER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnReserveOfferOperation();
            }
        };
        constructors[TXN_RESERVE_POLL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnReservePollOperation();
            }
        };
        constructors[TXN_ROLLBACK_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnRollbackBackupOperation();
            }
        };
        constructors[TXN_ROLLBACK] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnRollbackOperation();
            }
        };
        constructors[CHECK_EVICT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CheckAndEvictOperation();
            }
        };
        constructors[QUEUE_CONTAINER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new QueueContainer(null);
            }
        };
        constructors[TRANSACTION_ROLLBACK] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new QueueTransactionRollbackOperation();
            }
        };
        constructors[TX_QUEUE_ITEM] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxQueueItem();
            }
        };
        constructors[TXN_PEEK] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnPeekOperation();
            }
        };


        return new ArrayDataSerializableFactory(constructors);
    }
}
