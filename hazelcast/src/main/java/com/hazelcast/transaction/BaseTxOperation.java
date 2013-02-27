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
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

/**
 * @mdogan 2/26/13
 */
abstract class BaseTxOperation extends Operation implements PartitionAwareOperation {

    protected String txnId;
    protected String[] services;

    protected BaseTxOperation() {
    }

    protected BaseTxOperation(String txnId, String[] services) {
        this.txnId = txnId;
        this.services = services;
    }

    public final void beforeRun() throws Exception {
    }

    public final void afterRun() throws Exception {
    }

    public final boolean returnsResponse() {
        return true;
    }

    public final Object getResponse() {
        return Boolean.TRUE;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(txnId);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        txnId = in.readUTF();
    }
}
