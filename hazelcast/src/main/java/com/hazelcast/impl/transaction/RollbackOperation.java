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

import com.hazelcast.impl.spi.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RollbackOperation extends AbstractOperation {
    String txnId = null;

    public RollbackOperation(String txnId) {
        this.txnId = txnId;
    }

    public RollbackOperation() {
    }

    public void run() {
        OperationContext context = getOperationContext();
        TransactionalService txnalService = (TransactionalService) context.getService();
        ResponseHandler responseHandler = context.getResponseHandler();
        try {
            txnalService.rollback(txnId, context.getPartitionId());
            responseHandler.sendResponse(null);
        } catch (TransactionException e) {
            responseHandler.sendResponse(e);
        }
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        super.writeData(out);
        out.writeUTF(txnId);
    }

    @Override
    public void readData(DataInput in) throws IOException {
        super.readData(in);
        txnId = in.readUTF();
    }
}
