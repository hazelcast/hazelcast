/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.map.client;

import com.hazelcast.instance.Node;
import com.hazelcast.map.MapService;
import com.hazelcast.map.proxy.DataMapProxy;
import com.hazelcast.nio.Protocol;

import java.util.concurrent.TimeUnit;

public class MapTryPutHandler extends MapCommandHandler {
    public MapTryPutHandler(MapService mapService) {
        super(mapService);
    }

    @Override
    public Protocol processCall(Node node, Protocol protocol) {
        String[] args = protocol.args;
        String name = protocol.args[0];
        byte[] key = protocol.buffers[0].array();
        byte[] value = protocol.buffers[1].array();
        DataMapProxy dataMapProxy = (DataMapProxy) mapService.createClientProxy(name);
        final long ttl = (args.length > 1) ? Long.valueOf(args[1]) : 0;
        boolean isPut = dataMapProxy.tryPut(binaryToData(key), protocol.buffers.length > 1 ? binaryToData(value) : null, ttl, TimeUnit.MILLISECONDS);
        return protocol.success(String.valueOf(isPut));
    }
}
