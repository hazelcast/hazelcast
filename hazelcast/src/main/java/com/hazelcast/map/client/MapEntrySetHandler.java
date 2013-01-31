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

package com.hazelcast.map.client;

import com.hazelcast.instance.Node;
import com.hazelcast.map.MapService;
import com.hazelcast.map.proxy.DataMapProxy;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;

import java.util.Map;
import java.util.Set;

public class MapEntrySetHandler extends MapCommandHandler {
    public MapEntrySetHandler(MapService mapService) {
        super(mapService);
    }

    @Override
    public Protocol processCall(Node node, Protocol protocol) {
        String name = protocol.args[0];
        Predicate predicate = null;
        if(protocol.buffers.length > 0){
            predicate = (Predicate) node.serializationService.toObject(protocol.buffers[0]);
        }
        DataMapProxy dataMapProxy = getMapProxy(name);
        Set<Map.Entry<Data, Data>> entries;
        System.out.println("Predicate is " + predicate);
        if(predicate == null)
            entries= dataMapProxy.entrySet();
        else
            entries = dataMapProxy.entrySet(predicate);
        System.out.println(entries);
        Data[] arrayEntries = new Data[entries.size() * 2];
        int i = 0;
        for (Map.Entry<Data, Data> entry : entries) {
            arrayEntries[i++] = entry.getKey();
            arrayEntries[i++] = entry.getValue();
        }
        return protocol.success(arrayEntries);
    }
}
