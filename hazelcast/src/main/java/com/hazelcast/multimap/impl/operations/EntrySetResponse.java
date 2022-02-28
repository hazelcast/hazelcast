/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngine;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.SetUtil.createHashSet;

public class EntrySetResponse implements IdentifiedDataSerializable {

    private Map<Data, Collection<Data>> map;

    public EntrySetResponse() {
    }

    public EntrySetResponse(Map<Data, Collection<MultiMapRecord>> map, NodeEngine nodeEngine) {
        this.map = createHashMap(map.size());
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
        Set<Map.Entry<Data, Data>> entrySet = createHashSet(map.size() * 2);
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
        Set<Map.Entry<K, V>> entrySet = createHashSet(map.size() * 2);
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

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<Data, Collection<Data>> entry : map.entrySet()) {
            IOUtil.writeData(out, entry.getKey());
            Collection<Data> coll = entry.getValue();
            out.writeInt(coll.size());
            for (Data data : coll) {
                IOUtil.writeData(out, data);
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        map = createHashMap(size);
        for (int i = 0; i < size; i++) {
            Data key = IOUtil.readData(in);
            int collSize = in.readInt();
            Collection<Data> coll = new ArrayList<Data>(collSize);
            for (int j = 0; j < collSize; j++) {
                coll.add(IOUtil.readData(in));
            }
            map.put(key, coll);
        }
    }

    @Override
    public int getFactoryId() {
        return MultiMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.ENTRY_SET_RESPONSE;
    }
}
