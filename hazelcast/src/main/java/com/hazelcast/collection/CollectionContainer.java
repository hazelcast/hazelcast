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

package com.hazelcast.collection;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @ali 1/2/13
 */
public class CollectionContainer {

    String name;

    CollectionService service;

    private final ConcurrentMap<Data, Object> objects = new ConcurrentHashMap<Data, Object>(1000);

    public CollectionContainer(String name, CollectionService service) {
        this.name = name;
        this.service = service;
    }

    public Object getObject(Data dataKey){
        return objects.get(dataKey);
    }

    public void removeObject(Data dataKey){
        objects.remove(dataKey);
    }

    public Object putNewObject(Data dataKey){
        Object obj = service.createNew(name);
        objects.put(dataKey, obj);
        return obj;
    }

    public Set<Data> keySet(){
        Set<Data> keySet = objects.keySet();
        Set<Data> keys = new HashSet<Data>(keySet.size());
        keys.addAll(keySet);
        return keys;
    }

    public Collection values(){
        //TODO can we lock per key
        List valueList = new LinkedList();
        for (Object obj: objects.values()){
            valueList.addAll((Collection) obj);
        }
        return valueList;
    }

    public boolean containsKey(Data key){
        return objects.containsKey(key);
    }

    public boolean containsEntry(boolean binary, Data key, Data value){
        Collection coll = (Collection)objects.get(key);
        if (coll == null){
            return false;
        }
        return coll.contains(binary ? value : getNodeEngine().toObject(value));
    }

    public boolean containsValue(boolean binary, Data value){
        for (Data key: objects.keySet()){
            if (containsEntry(binary, key, value)){
                return true;
            }
        }
        return false;
    }

    public Map<Data, Collection> entrySet(){
        Map<Data, Collection> map = new HashMap<Data, Collection>(objects.size());
        for (Map.Entry<Data, Object> entry: objects.entrySet()){
            Data key = entry.getKey();
            Collection col = copyCollection((Collection)entry.getValue());
            map.put(key, col);
        }
        return map;
    }

    private Collection copyCollection(Collection coll){
        Collection copy = new ArrayList(coll.size());
        copy.addAll(coll);
        return copy;
    }

    public int size(){
        int size = 0;
        for (Object obj: objects.values()){
            size += ((Collection)obj).size();
        }
        return size;
    }

    public void clear(){
        objects.clear();
    }

    public NodeEngine getNodeEngine(){
        return service.getNodeEngine();
    }

    public String getName() {
        return name;
    }

    public ConcurrentMap<Data, Object> getObjects() {
        return objects; //TODO for testing only
    }
}
