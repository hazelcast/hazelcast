/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.transaction;

import com.hazelcast.impl.spi.Operation;
import com.hazelcast.impl.spi.ResponseHandler;
import com.hazelcast.impl.spi.TransactionException;
import com.hazelcast.impl.spi.TransactionalService;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CommitOperation extends Operation {
    String txnId = null;

    public CommitOperation(String txnId) {
        this.txnId = txnId;
    }

    public CommitOperation() {
    }

    public void run() {
        TransactionalService txnalService = (TransactionalService) getService();
        ResponseHandler responseHandler = getResponseHandler();
        try {
            txnalService.commit(txnId, getPartitionId());
            responseHandler.sendResponse(null);
        } catch (TransactionException e) {
            responseHandler.sendResponse(e);
        }
    }

    @Override
    public void writeInternal(DataOutput out) throws IOException {
        super.writeData(out);
        out.writeUTF(txnId);
    }

    @Override
    public void readInternal(DataInput in) throws IOException {
        super.readData(in);
        txnId = in.readUTF();
    }
}
