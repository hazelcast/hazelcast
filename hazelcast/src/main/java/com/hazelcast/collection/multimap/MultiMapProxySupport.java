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

package com.hazelcast.collection.multimap;

import com.hazelcast.collection.CollectionService;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.Data;
import com.hazelcast.spi.NodeEngine;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * @ali 1/2/13
 */
public abstract class MultiMapProxySupport{

    final String name;

    final CollectionService service;

    final NodeEngine nodeEngine;

    protected final MultiMapConfig config;

    protected MultiMapProxySupport(String name, CollectionService service, NodeEngine nodeEngine) {
        this.name = name;
        this.service = service;
        this.nodeEngine = nodeEngine;
        config = new MultiMapConfig(nodeEngine.getConfig().getMultiMapConfig(name));
    }

    public Object createNew() {
        if(config.getValueCollectionType().equals(MultiMapConfig.ValueCollectionType.SET)){
            return new HashSet(10);//TODO hardcoded initial
        }
        else if (config.getValueCollectionType().equals(MultiMapConfig.ValueCollectionType.SET)){
            return new LinkedList();
        }
        throw new IllegalArgumentException("No Matching CollectionProxyType!");
    }

    Boolean putInternal(Data dataKey, Data dataValue){
        return service.process(name, dataKey, new PutEntryProcessor(dataValue, config.isBinary()));
    }

    MultiMapCollectionResponse getInternal(Data dataKey){
        return service.process(name, dataKey, new GetEntryProcessor(config.isBinary(), config.getValueCollectionType()));
    }

    Boolean removeInternal(Data dataKey, Data dataValue){
        return service.process(name, dataKey, new RemoveObjectEntryProcess(dataValue, config.isBinary()));
    }

    MultiMapCollectionResponse removeInternal(Data dataKey){
        return service.process(name, dataKey, new RemoveEntryProcessor(config.isBinary(), config.getValueCollectionType()));
    }

    Set<Data> localKeySetInternal(){
        return service.localKeySet(name);
    }

    Set<Data> keySetInternal(){
        try {
            KeySetOperation operation = new KeySetOperation(name);
            Map<Integer, Object> results = nodeEngine.getOperationService().invokeOnAllPartitions(CollectionService.COLLECTION_SERVICE_NAME, operation, false);
            Set<Data> keySet = new HashSet<Data>();
            for (Object result : results.values()) {
                if (result == null) {
                    continue;
                }
                MultiMapCollectionResponse response = (MultiMapCollectionResponse)nodeEngine.toObject(result);
                keySet.addAll(response.getDataCollection());
            }
            return keySet;
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }








}
