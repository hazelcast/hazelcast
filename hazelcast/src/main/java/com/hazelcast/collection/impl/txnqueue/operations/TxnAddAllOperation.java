/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.txnqueue.operations;

import com.hazelcast.collection.impl.queue.QueueContainer;
import com.hazelcast.collection.impl.queue.QueueDataSerializerHook;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.internal.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.transaction.TransactionalQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Transaction commit operation for a queue add all, executed on the primary replica.
 *
 * @see TransactionalQueue#addAll(Collection)
 * @see TxnReserveAddAllOperation
 */
public class TxnAddAllOperation extends BaseTxnQueueOperation implements Notifier, MutatingOperation {

    private Set<Long> itemIds;
    private List<Data> data;

    public TxnAddAllOperation() {
    }

    public TxnAddAllOperation(String name, Set<Long> itemIds, List<Data> data) {
        super(name, -1);
        this.itemIds = itemIds;
        this.data = data;
    }

    @Override
    public void run() throws Exception {
        QueueContainer createContainer = getContainer();
        response = createContainer.txnCommitAddAll(itemIds, data, false);
    }

    @Override
    public void afterRun() throws Exception {
        LocalQueueStatsImpl queueStats = getQueueService().getLocalQueueStatsImpl(name);
        if (Boolean.TRUE.equals(response)) {
            data.forEach(
                d -> {
                    queueStats.incrementOffers();
                    publishEvent(ItemEventType.ADDED, d);
                }
            );
        } else {
            data.forEach(_d -> queueStats.incrementRejectedOffers());
        }
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new TxnAddAllBackupOperation(name, itemIds, data);
    }

    @Override
    public boolean shouldNotify() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return getContainer().getPollWaitNotifyKey();
    }

    @Override
    public boolean isRemoveOperation() {
        return false;
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.TXN_ADD_ALL;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(data.size());
        for (Data datum : data) {
            IOUtil.writeData(out, datum);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            data.add(IOUtil.readData(in));
        }
    }
}
