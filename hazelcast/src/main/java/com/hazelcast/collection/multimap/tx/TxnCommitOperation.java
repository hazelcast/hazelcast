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

package com.hazelcast.collection.multimap.tx;

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionWrapper;
import com.hazelcast.collection.operations.CollectionBackupAwareOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ali 4/12/13
 */
public class TxnCommitOperation extends CollectionBackupAwareOperation implements Notifier {

    List<Operation> opList;
    long version;
    transient boolean notify = true;

    public TxnCommitOperation() {
    }

    public TxnCommitOperation(CollectionProxyId proxyId, Data dataKey, int threadId, long version, List<Operation> opList) {
        super(proxyId, dataKey, threadId);
        this.version = version;
        this.opList = opList;
    }

    public void run() throws Exception {
        CollectionWrapper wrapper = getCollectionWrapper();
        if (wrapper == null || wrapper.getVersion() != version){
            notify = false;
            return;
        }
        wrapper.incrementAndGetVersion();
        for (Operation op: opList){
            op.setNodeEngine(getNodeEngine()).setServiceName(getServiceName()).setPartitionId(getPartitionId());
            op.beforeRun();
            op.run();
            op.afterRun();
        }
        getOrCreateContainer().unlock(dataKey, getCallerUuid(), threadId);
    }

    public boolean shouldBackup() {
        return notify;
    }

    public Operation getBackupOperation() {
        return new TxnCommitBackupOperation(proxyId, dataKey, opList, getCallerUuid(), threadId);
    }

    public boolean shouldNotify() {
        return notify;
    }

    public WaitNotifyKey getNotifiedKey() {
        return getWaitKey();
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(version);
        out.writeInt(opList.size());
        for (Operation op: opList){
            out.writeObject(op);
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        version = in.readLong();
        int size = in.readInt();
        opList = new ArrayList<Operation>(size);
        for (int i=0; i<size; i++){
            opList.add((Operation)in.readObject());
        }
    }

    public int getId() {
        return CollectionDataSerializerHook.TXN_COMMIT;
    }
}
