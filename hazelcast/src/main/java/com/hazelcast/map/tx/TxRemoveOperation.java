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
import com.hazelcast.nio.serialization.Data;

public class TxRemoveOperation extends BaseTxRemoveOperation {

    transient boolean successful = false;

    public TxRemoveOperation() {
    }

    public TxRemoveOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    protected void innerProcess() {
        TransactionItem transactionItem = partitionContainer.addTransactionItem(new TransactionItem(getTransactionId(), name, getKey(), getValue(), true));
        if (transactionItem != null && !transactionItem.isRemoved()){
            dataOldValue = transactionItem.getValue();
        }
        else {
            dataOldValue = mapService.toData(recordStore.get(dataKey));
        }
    }

    protected void innerOnCommit() {
        partitionContainer.removeTransactionItem(new TransactionKey(getTransactionId(), name, dataKey));
        dataOldValue = mapService.toData(recordStore.remove(dataKey));
        successful = dataOldValue != null;
    }

    protected void innerOnRollback() {
        partitionContainer.removeTransactionItem(new TransactionKey(getTransactionId(), name, dataKey));
    }

    public void innerAfterRun() throws Exception {
        if (successful) {
            super.innerAfterRun();
        }
    }

    public boolean shouldBackup() {
        return successful && isCommitted();
    }

    @Override
    public String toString() {
        return "TxRemoveOperation{" + name + "}";
    }

}
