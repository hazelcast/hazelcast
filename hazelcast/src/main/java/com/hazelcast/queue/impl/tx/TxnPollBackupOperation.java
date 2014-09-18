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

package com.hazelcast.queue.impl.tx;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.queue.impl.QueueDataSerializerHook;
import com.hazelcast.queue.impl.QueueOperation;

import java.io.IOException;

/**
 * Provides backup operation during transactional poll operation.
 */
public class TxnPollBackupOperation extends QueueOperation {

    long itemId;

    public TxnPollBackupOperation() {
    }

    public TxnPollBackupOperation(String name, long itemId) {
        super(name);
        this.itemId = itemId;
    }

    @Override
    public void run() throws Exception {
        getOrCreateContainer().txnCommitPollBackup(itemId);
        response = true;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(itemId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemId = in.readLong();
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.TXN_POLL_BACKUP;
    }

}
