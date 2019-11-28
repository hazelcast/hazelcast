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
import com.hazelcast.collection.impl.queue.operations.QueueOperation;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Provides backup operation during transactional add all operation.
 */
public class TxnAddAllBackupOperation extends QueueOperation implements BackupOperation {

    private Set<Long> itemIds;
    private List<Data> data;

    public TxnAddAllBackupOperation() {
    }

    public TxnAddAllBackupOperation(String name, Set<Long> itemIds, List<Data> data) {
        super(name);
        this.itemIds = itemIds;
        this.data = data;
    }

    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getContainer();
        int i = 0;
        for (Long itemId : itemIds) {
            Data datum = data.get(i);
            queueContainer.txnCommitOffer(itemId, datum, true);
            i++;
        }
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.TXN_OFFER_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(itemIds.size());
        for (Long itemId : itemIds) {
            out.writeLong(itemId);
        }
        for (Data datum : data) {
            IOUtil.writeData(out, datum);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        itemIds = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            itemIds.add(in.readLong());
        }
        data = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            data.add(IOUtil.readData(in));
        }
    }
}
