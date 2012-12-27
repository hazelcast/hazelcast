/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TransactionLog implements DataSerializable {
    String txnId;
    final Map<Data, TransactionLogItem> changes = new HashMap<Data, TransactionLogItem>(2);

    public TransactionLog(String txnId) {
        this.txnId = txnId;
    }

    public TransactionLog() {
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(txnId);
        int size = changes.size();
        out.writeInt(size);
        for (Map.Entry<Data, TransactionLogItem> entry : changes.entrySet()) {
            entry.getKey().writeData(out);
            entry.getValue().writeData(out);
        }
    }

    public void readData(DataInput in) throws IOException {
        txnId = in.readUTF();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Data key = new Data();
            key.readData(in);
            TransactionLogItem logItem = new TransactionLogItem();
            logItem.readData(in);
            changes.put(key, logItem);
        }
    }

    public void addLogItem(TransactionLogItem logItem) {
        changes.put(logItem.getKey(), logItem);
    }
}
