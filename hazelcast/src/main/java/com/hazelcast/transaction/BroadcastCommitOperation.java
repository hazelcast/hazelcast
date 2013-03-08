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
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.util.logging.Level;

/**
 * @mdogan 2/27/13
 */

public class BroadcastCommitOperation extends AbstractOperation {

    private String txnId;

    public BroadcastCommitOperation() {
    }

    public BroadcastCommitOperation(String txnId) {
        this.txnId = txnId;
    }

    public void run() throws Exception {
        final NodeEngine nodeEngine = getNodeEngine();
        nodeEngine.getLogger(getClass()).log(Level.INFO, "Committing transaction[" + txnId + "]!");
        TransactionManagerServiceImpl service = getService();
        try {
            service.commitAll(txnId);
        } catch (Exception e) {
            nodeEngine.getLogger(getClass()).log(Level.WARNING, e.getMessage(), e);
        }
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(txnId);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        txnId = in.readUTF();
    }
}
