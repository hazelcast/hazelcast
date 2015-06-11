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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ResponseHandler;

import javax.transaction.xa.XAException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class FinalizeRemoteTransactionOperation extends Operation implements BackupAwareOperation {

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
        final ResponseHandler responseHandler = getResponseHandler();
        XAService xaService = getService();
        final List<XATransactionImpl> list = xaService.removeTransactions(xid);
        if (list == null) {
            responseHandler.sendResponse(getNodeEngine().toData(XAException.XAER_NOTA));
            return;
        }
        final int size = list.size();

        final ExecutionCallback callback = new ExecutionCallback() {
            AtomicInteger counter = new AtomicInteger();

            @Override
            public void onResponse(Object response) {
                sendResponseIfComplete();
            }

            @Override
            public void onFailure(Throwable t) {
                // TODO log the error
                sendResponseIfComplete();
            }

            void sendResponseIfComplete() {
                if (size == counter.incrementAndGet()) {
                    responseHandler.sendResponse(null);
                }
            }
        };

        for (XATransactionImpl xaTransaction : list) {
            finalizeTransaction(xaTransaction, callback);
        }

    }

    private void finalizeTransaction(XATransactionImpl xaTransaction, ExecutionCallback callback) {
        if (isCommit) {
            xaTransaction.commitAsync(callback);
        } else {
            xaTransaction.rollbackAsync(callback);
        }
    }

    @Override
    public void afterRun() throws Exception {

    }

    @Override
    public boolean returnsResponse() {
        return returnsResponse;
    }

    @Override
    public Object getResponse() {
        return null;
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
    public String getServiceName() {
        return XAService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeData(xidData);
        out.writeBoolean(isCommit);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        xidData = in.readData();
        isCommit = in.readBoolean();
    }
}
