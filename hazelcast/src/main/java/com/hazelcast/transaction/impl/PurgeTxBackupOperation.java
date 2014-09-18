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

package com.hazelcast.transaction.impl;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.exception.TargetNotMemberException;

import java.io.IOException;

public final class PurgeTxBackupOperation extends Operation {

    private String txnId;

    public PurgeTxBackupOperation() {
    }

    public PurgeTxBackupOperation(String txnId) {
        this.txnId = txnId;
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        TransactionManagerServiceImpl txManagerService = getService();
        txManagerService.purgeTxBackupLog(txnId);
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public ExceptionAction onException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onException(throwable);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(txnId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        txnId = in.readUTF();
    }
}
