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

package com.hazelcast.impl;

import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class Keys implements DataSerializable {

    private Collection<Data> keys;

    public Keys() {
        keys = new ArrayList<Data>();
    }

    public Keys(Collection<Data> keys) {
        this.keys = keys;
    }

    public Collection<Data> getKeys() {
        return this.keys;
    }

    public void readData(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Data data = new Data();
            data.readData(in);
            keys.add(data);
        }
    }

    public void writeData(DataOutput out) throws IOException {
        int size = (keys == null) ? 0 : keys.size();
        out.writeInt(size);
        if (size > 0) {
            for (Data key : keys) {
                key.writeData(out);
            }
        }
    }

    public void addKey(Data obj) {
        this.keys.add(obj);
    }
}
