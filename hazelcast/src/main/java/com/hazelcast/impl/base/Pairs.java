/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
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

    public void addKeyValue(KeyValue keyValue) {
        if (getLsKeyValues() == null) {
            setLsKeyValues(new ArrayList<KeyValue>());
        }
        getLsKeyValues().add(keyValue);
    }

    public void writeData(DataOutput out) throws IOException {
        int size = (getLsKeyValues() == null) ? 0 : getLsKeyValues().size();
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            getLsKeyValues().get(i).writeData(out);
        }
    }

    public void readData(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            if (getLsKeyValues() == null) {
                setLsKeyValues(new ArrayList<KeyValue>());
            }
            KeyValue kv = new KeyValue();
            kv.readData(in);
            getLsKeyValues().add(kv);
        }
    }

    public int size() {
        return (getLsKeyValues() == null) ? 0 : getLsKeyValues().size();
    }

    public KeyValue getEntry(int i) {
        return (getLsKeyValues() == null) ? null : getLsKeyValues().get(i);
    }

    /**
     * @param lsKeyValues the lsKeyValues to set
     */
    public void setLsKeyValues(List<KeyValue> lsKeyValues) {
        this.lsKeyValues = lsKeyValues;
    }

    /**
     * @return the lsKeyValues
     */
    public List<KeyValue> getLsKeyValues() {
        return lsKeyValues;
    }
}
