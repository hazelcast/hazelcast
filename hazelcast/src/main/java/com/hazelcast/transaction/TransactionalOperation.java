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

package com.hazelcast.transaction;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.ResponseHandlerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @mdogan 3/4/13
 */

public abstract class TransactionalOperation extends Operation implements DataSerializable {

    private String txnId = null;

    private transient Transaction.State state = Transaction.State.ACTIVE;

    protected TransactionalOperation() {
    }

    protected TransactionalOperation(String txnId) {
        this.txnId = txnId;
    }

    public final void run() throws Exception {
        switch (state) {
            case ACTIVE:
                process();
                addTransactionRecord();
                break;

            case PREPARED:
                onPrepare();
                break;

            case COMMITTED:
                onCommit();
                break;

            case ROLLED_BACK:
                onRollback();
                break;

            default:
                throw new IllegalStateException();
        }
    }

    private void addTransactionRecord() throws TransactionException {
        final TransactionManagerServiceImpl txService = (TransactionManagerServiceImpl) getNodeEngine().getTransactionManagerService();
        txService.addTransactionalOperation(getPartitionId(), this);
    }

    public final void beforeRun() throws Exception {
        if (state == Transaction.State.ACTIVE) {
            innerBeforeRun();
        }
    }

    public final void afterRun() throws Exception {
        if (state == Transaction.State.COMMITTED) {
            innerAfterRun();
        }
    }

    protected void innerBeforeRun() throws Exception {
    }

    protected void innerAfterRun() throws Exception {
    }

    final void prepare() throws TransactionException {
        state = Transaction.State.PREPARED;
        ResponseHandlerFactory.FutureResponseHandler f = new ResponseHandlerFactory.FutureResponseHandler();
        setResponseHandler(f);
        getNodeEngine().getOperationService().runOperation(this);
        try {
            // no need to block thread if op doesn't return response.
            f.get(1, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new TransactionException(e);
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof TransactionException) {
                throw (TransactionException) t;
            } else {
                throw new TransactionException(t);
            }
        } catch (TimeoutException ignored) {
        }
    }

    final void commit() {
        completeTx(Transaction.State.COMMITTED);
    }

    final void rollback() {
        completeTx(Transaction.State.ROLLED_BACK);
    }

    private void completeTx(Transaction.State state) {
        this.state = state;
        final NodeEngine nodeEngine = getNodeEngine();
        setResponseHandler(ResponseHandlerFactory.createErrorLoggingResponseHandler(nodeEngine.getLogger(getClass())));
        nodeEngine.getOperationService().runOperation(this);
    }

    public final String getTransactionId() {
        return txnId;
    }

    public final void setTransactionId(String txnId) {
        this.txnId = txnId;
    }

    public final Transaction.State getState() {
        return state;
    }

    void setState(Transaction.State state) {
        this.state = state;
    }

    public boolean returnsResponse() {
        return true;
    }

    protected final boolean isCommitted() {
        return state == Transaction.State.COMMITTED;
    }

    protected abstract void process() throws TransactionException;

    protected abstract void onPrepare() throws TransactionException;

    protected abstract void onCommit();

    protected abstract void onRollback();

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(txnId);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        txnId = in.readUTF();
    }

}
