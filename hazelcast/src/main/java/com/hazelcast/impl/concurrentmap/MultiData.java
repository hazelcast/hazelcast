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

package com.hazelcast.impl.concurrentmap;

import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiData implements DataSerializable {
    List<Data> lsData = null;

    public MultiData() {
    }

    public MultiData(Data d1, Data d2) {
        lsData = new ArrayList<Data>(2);
        lsData.add(d1);
        lsData.add(d2);
    }

    public void writeData(DataOutput out) throws IOException {
        int size = lsData.size();
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            Data d = lsData.get(i);
            d.writeData(out);
        }
    }

    public void readData(DataInput in) throws IOException {
        int size = in.readInt();
        lsData = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            Data data = new Data();
            data.readData(in);
            lsData.add(data);
        }
    }

    public int size() {
        return (lsData == null) ? 0 : lsData.size();
    }

    public List<Data> getAllData() {
        return lsData;
    }

    public Data getData(int index) {
        return (lsData == null) ? null : lsData.get(index);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("MultiData");
        sb.append("{size=").append(size());
        sb.append('}');
        return sb.toString();
    }
}
