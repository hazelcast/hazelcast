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

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.transaction.impl.TransactionDataSerializerHook;
import com.hazelcast.transaction.impl.xa.SerializableXID;
import com.hazelcast.transaction.impl.xa.XAService;
import com.hazelcast.transaction.impl.xa.XATransaction;

import javax.transaction.xa.XAException;
import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;

public class FinalizeRemoteTransactionOperation extends AbstractXAOperation implements BackupAwareOperation {

    private Data xidData;
    private boolean isCommit;

    private transient boolean returnsResponse = true;
    private transient SerializableXID xid;

    public FinalizeRemoteTransactionOperation() {
    }

    public FinalizeRemoteTransactionOperation(Data xidData, boolean isCommit) {
        this.xidData = xidData;
        this.isCommit = isCommit;
    }

    @Override
    public void beforeRun() throws Exception {
        returnsResponse = false;
        xid = getNodeEngine().toObject(xidData);
    }

    @Override
    public void run() throws Exception {
        XAService xaService = getService();
        final List<XATransaction> list = xaService.removeTransactions(xid);
        if (list == null) {
            sendResponse(getNodeEngine().toData(XAException.XAER_NOTA));
            return;
        }
        final int size = list.size();

        final TransactionFinalizationCallback callback = new TransactionFinalizationCallback(this, size);

        for (XATransaction xaTransaction : list) {
            finalizeTransaction(xaTransaction, callback);
        }
    }

    private void finalizeTransaction(XATransaction xaTransaction, BiConsumer callback) {
        if (isCommit) {
            xaTransaction.commitAsync(callback);
        } else {
            xaTransaction.rollbackAsync(callback);
        }
    }

    @Override
    public boolean returnsResponse() {
        return returnsResponse;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public int getSyncBackupCount() {
        return 0;
    }

    @Override
    public int getAsyncBackupCount() {
        return 1;
    }

    @Override
    public Operation getBackupOperation() {
        return new FinalizeRemoteTransactionBackupOperation(xidData);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        IOUtil.writeData(out, xidData);
        out.writeBoolean(isCommit);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        xidData = IOUtil.readData(in);
        isCommit = in.readBoolean();
    }

    @Override
    public int getClassId() {
        return TransactionDataSerializerHook.FINALIZE_REMOTE_TX;
    }
}
