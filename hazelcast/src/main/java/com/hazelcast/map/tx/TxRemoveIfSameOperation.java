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

package com.hazelcast.map.tx;

import com.hazelcast.map.TransactionItem;
import com.hazelcast.map.TransactionKey;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

public class TxRemoveIfSameOperation extends BaseTxRemoveOperation {

    private Data testValue;
    transient boolean successful = false;

    public TxRemoveIfSameOperation() {
    }

    public TxRemoveIfSameOperation(String name, Data dataKey, Data testValue) {
        super(name, dataKey);
        this.testValue = testValue;
    }

    protected void innerProcess() {
        TransactionItem transactionItem = partitionContainer.getTransactionItem(new TransactionKey(getTransactionId(), name, dataKey));
        if (transactionItem != null && !transactionItem.isRemoved()) {
            successful = mapService.compare(name, transactionItem.getValue(), testValue);
        } else {
            Object value = recordStore.get(dataKey);
            successful = mapService.compare(name, testValue, value);
        }
        if (successful) {
            partitionContainer.addTransactionItem(new TransactionItem(getTransactionId(), name, getKey(), getValue(), true));
        }
    }

    protected void innerOnCommit() {
        recordStore.remove(dataKey,testValue);
        if (successful) {
            partitionContainer.removeTransactionItem(new TransactionKey(getTransactionId(), name, dataKey));
        }
    }

    protected void innerOnRollback() {
        if (successful) {
            partitionContainer.removeTransactionItem(new TransactionKey(getTransactionId(), name, dataKey));
        }
    }

    public void innerAfterRun() throws Exception {
        if (successful) {
            super.innerAfterRun();
        }
    }

    public boolean shouldBackup() {
        return successful && isCommitted();
    }

    public Object getResponse() {
        return successful;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeNullableData(out, testValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        testValue = IOUtil.readNullableData(in);
    }

    @Override
    public String toString() {
        return "TxRemoveOperation{" + name + "}";
    }

}
