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

package com.hazelcast.collection.impl.txnqueue.operations;

import com.hazelcast.collection.impl.CollectionTxnUtil;
import com.hazelcast.collection.impl.queue.QueueContainer;
import com.hazelcast.collection.impl.queue.QueueDataSerializerHook;
import com.hazelcast.collection.impl.queue.operations.QueueBackupAwareOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

import java.io.IOException;
import java.util.List;

/**
 * a wrapper for running all commit operations at once
 */
public class TxnCommitOperation extends QueueBackupAwareOperation implements Notifier {

    private List<Operation> operationList;

    private transient List<Operation> backupList;

    private transient long shouldNotify;

    public TxnCommitOperation() {
    }

    public TxnCommitOperation(int partitionId, String name, List<Operation> operationList) {
        super(name);
        setPartitionId(partitionId);
        this.operationList = operationList;
    }

    @Override
    public void beforeRun() throws Exception {
        super.beforeRun();
        CollectionTxnUtil.before(operationList, this);
    }

    @Override
    public void run() throws Exception {
        backupList = CollectionTxnUtil.run(operationList);
        for (Operation operation : operationList) {
            if (operation instanceof Notifier) {
                boolean shouldNotify = ((Notifier) operation).shouldNotify();
                if (shouldNotify) {
                    this.shouldNotify += operation instanceof TxnPollOperation ? +1 : -1;
                }
            }
        }
    }

    @Override
    public void afterRun() throws Exception {
        super.beforeRun();
        CollectionTxnUtil.after(operationList);
    }

    @Override
    public boolean shouldBackup() {
        return !backupList.isEmpty();
    }

    @Override
    public Operation getBackupOperation() {
        return new TxnCommitBackupOperation(name, backupList);
    }

    @Override
    public boolean shouldNotify() {
        return shouldNotify != 0;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        QueueContainer queueContainer = getContainer();
        if (CollectionTxnUtil.isRemove(shouldNotify)) {
            return queueContainer.getOfferWaitNotifyKey();
        }
        return queueContainer.getPollWaitNotifyKey();
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.TXN_COMMIT;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        CollectionTxnUtil.write(out, operationList);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        operationList = CollectionTxnUtil.read(in);
    }
}
