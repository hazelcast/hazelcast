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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.NodeEngine;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class EntrySetResponse implements DataSerializable {

    private Map<Data, Collection<Data>> map;

    public EntrySetResponse() {
    }

    public EntrySetResponse(Map<Data, Collection<MultiMapRecord>> map, NodeEngine nodeEngine) {
        this.map = new HashMap<Data, Collection<Data>>(map.size());
        for (Map.Entry<Data, Collection<MultiMapRecord>> entry : map.entrySet()) {
            Collection<MultiMapRecord> records = entry.getValue();
            Collection<Data> coll = new ArrayList<Data>(records.size());
            for (MultiMapRecord record : records) {
                coll.add(nodeEngine.toData(record.getObject()));
            }
            this.map.put(entry.getKey(), coll);
        }
    }

    public Set<Map.Entry<Data, Data>> getDataEntrySet() {
        Set<Map.Entry<Data, Data>> entrySet = new HashSet<Map.Entry<Data, Data>>();
        for (Map.Entry<Data, Collection<Data>> entry : map.entrySet()) {
            Data key = entry.getKey();
            Collection<Data> coll = entry.getValue();
            for (Data data : coll) {
                entrySet.add(new AbstractMap.SimpleEntry<Data, Data>(key, data));
            }
        }
        return entrySet;
    }

    public <K, V> Set<Map.Entry<K, V>> getObjectEntrySet(NodeEngine nodeEngine) {
        Set<Map.Entry<K, V>> entrySet = new HashSet<Map.Entry<K, V>>();
        for (Map.Entry<Data, Collection<Data>> entry : map.entrySet()) {
            K key = nodeEngine.toObject(entry.getKey());
            Collection<Data> coll = entry.getValue();
            for (Data data : coll) {
                V val = nodeEngine.toObject(data);
                entrySet.add(new AbstractMap.SimpleEntry<K, V>(key, val));
            }
        }
        return entrySet;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<Data, Collection<Data>> entry : map.entrySet()) {
            out.writeData(entry.getKey());
            Collection<Data> coll = entry.getValue();
            out.writeInt(coll.size());
            for (Data data : coll) {
                out.writeData(data);
            }
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        map = new HashMap<Data, Collection<Data>>(size);
        for (int i = 0; i < size; i++) {
            Data key = in.readData();
            int collSize = in.readInt();
            Collection coll = new ArrayList(collSize);
            for (int j = 0; j < collSize; j++) {
                coll.add(in.readData());
            }
            map.put(key, coll);
        }
    }
}
