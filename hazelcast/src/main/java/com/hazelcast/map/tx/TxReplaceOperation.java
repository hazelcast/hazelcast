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

public class TxReplaceOperation extends BaseTxPutOperation {
    transient boolean exists = false;

    public TxReplaceOperation() {
    }

    public TxReplaceOperation(String name, Data dataKey, Data dataValue) {
        super(name, dataKey, dataValue);
    }

    protected void innerProcess() {
        TransactionItem transactionItem = partitionContainer.getTransactionItem(new TransactionKey(getTransactionId(), name, dataKey));
        if(transactionItem != null && !transactionItem.isRemoved()) {
            dataOldValue = transactionItem.getValue();
            exists = true;
        } else {
            dataOldValue = mapService.toData(recordStore.get(dataKey));
            exists = dataOldValue != null;
        }
        if(exists) {
            partitionContainer.addTransactionItem(new TransactionItem(getTransactionId(), name, getKey(), getValue(), false));
        }
    }

    protected void innerOnCommit() {
        if (exists) {
            partitionContainer.removeTransactionItem(new TransactionKey(getTransactionId(), name, dataKey));
        }
        dataOldValue = mapService.toData(recordStore.replace(dataKey, dataValue));
    }

    protected void innerOnRollback() {
        if (exists) {
            partitionContainer.removeTransactionItem(new TransactionKey(getTransactionId(), name, dataKey));
        }
    }

    @Override
    public Object getResponse() {
        return dataOldValue;
    }

    @Override
    public String toString() {
        return "TxPutIfAbsentOperation{" + name + "}";
    }

}
