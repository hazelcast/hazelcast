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

package com.hazelcast.transaction.impl.xa.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.transaction.impl.TransactionDataSerializerHook;
import com.hazelcast.transaction.impl.xa.XAService;
import com.hazelcast.transaction.impl.xa.XATransaction;
import com.hazelcast.transaction.impl.xa.XATransactionDTO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class XaReplicationOperation extends Operation implements IdentifiedDataSerializable {

    private List<XATransactionDTO> migrationData;

    public XaReplicationOperation() {
    }

    public XaReplicationOperation(List<XATransactionDTO> migrationData, int partitionId, int replicaIndex) {
        setPartitionId(partitionId);
        setReplicaIndex(replicaIndex);
        this.migrationData = migrationData;
    }

    @Override
    public void run() throws Exception {
        XAService xaService = getService();
        NodeEngine nodeEngine = getNodeEngine();
        for (XATransactionDTO transactionDTO : migrationData) {
            XATransaction transaction = new XATransaction(
                    nodeEngine,
                    transactionDTO.getRecords(),
                    transactionDTO.getTxnId(),
                    transactionDTO.getXid(),
                    transactionDTO.getOwnerUuid(),
                    transactionDTO.getTimeoutMilis(),
                    transactionDTO.getStartTime());
            xaService.putTransaction(transaction);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (XATransactionDTO transactionDTO : migrationData) {
            out.writeObject(transactionDTO);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        migrationData = new ArrayList<XATransactionDTO>(size);
        for (int i = 0; i < size; i++) {
            XATransactionDTO transactionDTO = in.readObject();
            migrationData.add(transactionDTO);
        }
    }

    @Override
    public int getFactoryId() {
        return TransactionDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return TransactionDataSerializerHook.XA_REPLICATION;
    }
}
