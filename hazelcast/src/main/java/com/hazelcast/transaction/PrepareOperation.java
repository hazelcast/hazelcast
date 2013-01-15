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
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.spi.exception.TransactionException;

import java.io.IOException;

public class PrepareOperation extends AbstractOperation {
    String txnId = null;
    Object response = null;

    public PrepareOperation(String txnId) {
        this.txnId = txnId;
    }

    public PrepareOperation() {
    }

    public void run() {
        TransactionalService txnalService = (TransactionalService) getService();
        try {
            txnalService.prepare(txnId, getPartitionId());
        } catch (TransactionException e) {
            response = e;
        }
    }

    @Override
    public Object getResponse() {
        return response;
    }

    public void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(txnId);
    }

    public void readInternal(ObjectDataInput in) throws IOException {
        txnId = in.readUTF();
    }
}
