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

import java.util.Set;
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

    public void putObject(Data dataKey, Object object){
        objects.put(dataKey, object);
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
        return objects.keySet();
    }

    public ConcurrentMap<Data, Object> getObjects() {
        return objects; //TODO for testing only
    }
}
