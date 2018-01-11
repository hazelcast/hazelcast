/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.transaction.impl.TransactionDataSerializerHook;
import com.hazelcast.transaction.impl.xa.SerializableXID;
import com.hazelcast.transaction.impl.xa.XAService;

import java.io.IOException;

public class
ClearRemoteTransactionBackupOperation extends AbstractXAOperation implements BackupOperation {

    private Data xidData;

    private transient SerializableXID xid;

    public ClearRemoteTransactionBackupOperation() {
    }

    public ClearRemoteTransactionBackupOperation(Data xidData) {
        this.xidData = xidData;
    }

    @Override
    public void beforeRun() throws Exception {
        xid = getNodeEngine().toObject(xidData);
    }

    @Override
    public void run() throws Exception {
        XAService xaService = getService();
        xaService.removeTransactions(xid);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeData(xidData);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        xidData = in.readData();
    }

    @Override
    public int getId() {
        return TransactionDataSerializerHook.CLEAR_REMOTE_TX_BACKUP;
    }
}
