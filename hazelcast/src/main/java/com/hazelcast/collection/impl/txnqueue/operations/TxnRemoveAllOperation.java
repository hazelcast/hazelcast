/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.collection.impl.queue.QueueItem;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.internal.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class TxnRemoveAllOperation extends BaseTxnQueueOperation implements Notifier, MutatingOperation {
    private Collection<QueueItem> items;
    private Collection<Data> data;

    public TxnRemoveAllOperation() {
    }

    public TxnRemoveAllOperation(String name, Collection<Data> data) {
        super(name, -1);
        this.data = data;
    }

    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getContainer();
        items = queueContainer.txnCommitRemoveAll(data);
        response = !items.isEmpty();
    }

    @Override
    public void afterRun() throws Exception {
        LocalQueueStatsImpl queueStats = getQueueService().getLocalQueueStatsImpl(name);
        if (items.isEmpty()) {
            queueStats.incrementEmptyPolls();
        } else {
            for (QueueItem item : items) {
                queueStats.incrementPolls();
                publishEvent(ItemEventType.REMOVED, item.getData());
            }
        }
    }

    @Override
    public boolean shouldNotify() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        QueueContainer container = getContainer();
        return container.getRemoveAllWaitNotifyKey();
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public boolean isRemoveOperation() {
        return true;
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.TXN_REMOVE_ALL;
    }

    @Override
    public Operation getBackupOperation() {
        return new TxnRemoveAllBackupOperation(name, data);
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
        data = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            data.add(IOUtil.readData(in));
        }
    }
}
