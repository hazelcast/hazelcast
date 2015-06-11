/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.transaction.impl.xa;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class XaReplicationOperation extends AbstractOperation {

    List<XATransactionHolder> migrationData;

    public XaReplicationOperation() {
    }

    public XaReplicationOperation(List<XATransactionHolder> migrationData, int partitionId, int replicaIndex) {
        setPartitionId(partitionId);
        setReplicaIndex(replicaIndex);
        this.migrationData = migrationData;
    }


    @Override
    public void run() throws Exception {
        XAService xaService = getService();
        NodeEngine nodeEngine = getNodeEngine();
        for (XATransactionHolder holder : migrationData) {
            XATransactionImpl xaTransaction = new XATransactionImpl(nodeEngine, holder.txLogs, holder.txnId, holder.xid,
                    holder.ownerUuid, holder.timeoutMilis, holder.startTime);
            xaService.putTransaction(xaTransaction);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (XATransactionHolder xaTransactionHolder : migrationData) {
            out.writeObject(xaTransactionHolder);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        migrationData = new ArrayList<XATransactionHolder>(size);
        for (int i = 0; i < size; i++) {
            XATransactionHolder xaTransactionHolder = in.readObject();
            migrationData.add(xaTransactionHolder);
        }
    }

}
