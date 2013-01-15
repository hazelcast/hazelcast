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

package com.hazelcast.collection.multimap;

import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.util.*;

/**
 * @ali 1/8/13
 */
public class MultiMapResponse implements DataSerializable {

    private Map<Data, Collection> map;

    private transient SerializationService serializationService;

    public MultiMapResponse() {
    }

    public MultiMapResponse(Map<Data, Collection> map, SerializationService serializationService) {
        this.map = map;
        this.serializationService = serializationService;
    }

    public Set<Map.Entry<Data, Data>> getDataEntrySet(NodeEngine nodeEngine){
        Set<Map.Entry<Data, Data>> entrySet = new HashSet<Map.Entry<Data, Data>>();
        for (Map.Entry<Data, Collection> entry: map.entrySet()){
            Data key = entry.getKey();
            Collection coll = entry.getValue();
            for (Object obj: coll){
                Data data = nodeEngine.toData(obj);
                entrySet.add(new AbstractMap.SimpleEntry<Data, Data>(key, data));
            }
        }
        return entrySet;
    }

    public <K, V> Set<Map.Entry<K, V>> getObjectEntrySet(NodeEngine nodeEngine){
        Set<Map.Entry<K, V>> entrySet = new HashSet<Map.Entry<K, V>>();
        for (Map.Entry<Data, Collection> entry: map.entrySet()){
            K key = nodeEngine.toObject(entry.getKey());
            Collection coll = entry.getValue();
            for (Object obj: coll){
                V val = nodeEngine.toObject(obj);
                entrySet.add(new AbstractMap.SimpleEntry<K, V>(key, val));
            }
        }
        return entrySet;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<Data, Collection> entry: map.entrySet()){
            entry.getKey().writeData(out);
            Collection coll = entry.getValue();
            out.writeInt(coll.size());
            for (Object obj: coll){
                Data data = serializationService.toData(obj);
                data.writeData(out);
            }
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        map = new HashMap<Data, Collection>(size);
        for (int i=0; i<size; i++){
            Data key = IOUtil.readData(in);
            int collSize = in.readInt();
            Collection coll = new ArrayList(collSize);
            for (int j=0; j<collSize; j++){
                coll.add(IOUtil.readData(in));
            }
            map.put(key, coll);
        }
    }
}
