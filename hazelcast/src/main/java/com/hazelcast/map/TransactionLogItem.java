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

package com.hazelcast.map;

import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.IOUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TransactionLogItem implements DataSerializable {
    String name;
    Data key;
    Data value;
    boolean newEntry;
    boolean removed;

    public TransactionLogItem(String name, Data key, Data value, boolean newEntry, boolean removed) {
        this.name = name;
        this.key = key;
        this.value = value;
        this.newEntry = newEntry;
        this.removed = removed;
    }

    public TransactionLogItem() {
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(name);
        key.writeData(out);
        IOUtil.writeNullableData(out, value);
        out.writeBoolean(newEntry);
        out.writeBoolean(removed);
    }

    public void readData(DataInput in) throws IOException {
        name = in.readUTF();
        key = new Data();
        key.readData(in);
        value = IOUtil.readNullableData(in);
        newEntry = in.readBoolean();
        removed = in.readBoolean();
    }

    public String getName() {
        return name;
    }

    public boolean isRemoved() {
        return removed;
    }

    public Data getKey() {
        return key;
    }

    public Data getValue() {
        return value;
    }

    public boolean isNewEntry() {
        return newEntry;
    }

    @Override
    public String toString() {
        return "TransactionLogItem{" +
                "name='" + name + '\'' +
                ", key=" + key +
                ", value=" + value +
                ", newEntry=" + newEntry +
                ", removed=" + removed +
                '}';
    }
}
