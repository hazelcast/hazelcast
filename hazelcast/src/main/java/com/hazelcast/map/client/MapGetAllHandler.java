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
import com.hazelcast.nio.serialization.SerializationConstants;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MapGetAllHandler extends MapCommandHandler {
    public MapGetAllHandler(MapService mapService) {
        super(mapService);
    }

    @Override
    public Protocol processCall(Node node, Protocol protocol) {
        String name = protocol.args[0];
        int size = protocol.hasBuffer() ? protocol.buffers.length : 0;
        Set<Data> set = new HashSet<Data>();
        for (int i = 0; i < size; i++) {
            set.add(protocol.buffers[i]);
        }
        DataMapProxy dataMapProxy = (DataMapProxy) mapService.createDistributedObjectForClient(name);

        Map<Data, Data> result = dataMapProxy.getAll(set);
        Data[] buffers = new Data[size * 2];
        int i = 0;
        for(Map.Entry<Data, Data> entry: result.entrySet()){
            buffers[i++] = entry.getKey();
            buffers[i++] = entry.getValue();
        }
        return protocol.success(buffers);
    }
}

