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

package com.hazelcast.impl.base;

import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Pairs implements DataSerializable {
    private List<KeyValue> lsKeyValues = null;

    public Pairs() {
    }

    public Pairs(int size) {
        lsKeyValues = new ArrayList<KeyValue>(size);
    }

    public void addKeyValue(KeyValue keyValue) {
        if (getKeyValues() == null) {
            setKeyValues(new ArrayList<KeyValue>());
        }
        getKeyValues().add(keyValue);
    }

    public void writeData(DataOutput out) throws IOException {
        int size = (getKeyValues() == null) ? 0 : getKeyValues().size();
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            getKeyValues().get(i).writeData(out);
        }
    }

    public void readData(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            if (getKeyValues() == null) {
                setKeyValues(new ArrayList<KeyValue>());
            }
            KeyValue kv = new KeyValue();
            kv.readData(in);
            getKeyValues().add(kv);
        }
    }

    public int size() {
        return (getKeyValues() == null) ? 0 : getKeyValues().size();
    }

    public KeyValue getEntry(int i) {
        return (getKeyValues() == null) ? null : getKeyValues().get(i);
    }

    /**
     * @param lsKeyValues the lsKeyValues to set
     */
    public void setKeyValues(List<KeyValue> lsKeyValues) {
        this.lsKeyValues = lsKeyValues;
    }

    /**
     * @return the lsKeyValues
     */
    public List<KeyValue> getKeyValues() {
        return lsKeyValues;
    }

    @Override
    public String toString() {
        return "Pairs{" +
                "lsKeyValues=" + ((lsKeyValues == null) ? 0 : lsKeyValues.size()) +
                '}';
    }
}
