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

package com.hazelcast.map;

import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class TransactionItem implements DataSerializable {
    private String txnId;
    private String name;
    private Data key;
    private Data value;

    public TransactionItem() {
    }

    public TransactionItem(String txnId, String name, Data key, Data value) {
        this.txnId = txnId;
        this.name = name;
        this.key = key;
        this.value = value;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(txnId);
        out.writeUTF(name);
        key.writeData(out);
        IOUtil.writeNullableData(out, value);
    }

    public void readData(ObjectDataInput in) throws IOException {
        txnId = in.readUTF();
        name = in.readUTF();
        key = new Data();
        key.readData(in);
        value = IOUtil.readNullableData(in);
    }

    public String getTxnId() {
        return txnId;
    }

    public String getName() {
        return name;
    }

    public Data getKey() {
        return key;
    }

    public Data getValue() {
        return value;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("TransactionItem");
        sb.append("{txnId='").append(txnId).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", key=").append(key);
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }
}
