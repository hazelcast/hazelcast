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

package com.hazelcast.multimap;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @ali 1/2/13
 */
public class MultiMapPartitionContainer {

    MultiMapService service;

    private final ConcurrentMap<String, MultiMapContainer> multiMaps = new ConcurrentHashMap<String, MultiMapContainer>(1000);

    public MultiMapPartitionContainer(MultiMapService service) {
        this.service = service;
    }

    public MultiMapContainer getMultiMapContainer(String name){
        MultiMapContainer multiMap = multiMaps.get(name);
        if (multiMap == null){
            multiMap = new MultiMapContainer(name, service);
            MultiMapContainer current = multiMaps.putIfAbsent(name, multiMap);
            multiMap = current == null ? multiMap : current;
        }
        return multiMap;
    }

    public ConcurrentMap<String, MultiMapContainer> getMultiMaps() {
        return multiMaps; //TODO for testing only
    }
}
