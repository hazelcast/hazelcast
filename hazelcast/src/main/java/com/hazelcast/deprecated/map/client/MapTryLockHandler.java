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

package com.hazelcast.deprecated.map.client;

import com.hazelcast.instance.Node;
import com.hazelcast.map.MapService;
import com.hazelcast.map.proxy.DataMapProxy;
import com.hazelcast.deprecated.nio.Protocol;
import com.hazelcast.nio.serialization.Data;

import java.util.concurrent.TimeUnit;

public class MapTryLockHandler extends MapCommandHandler {
    public MapTryLockHandler(MapService mapService) {
        super(mapService);
    }

    @Override
    public Protocol processCall(Node node, Protocol protocol) {
        String name = protocol.args[0];
        long timeout;
        if (protocol.args.length > 0)
            timeout = Long.valueOf(protocol.args[1]);
        else
            timeout = 0;
        boolean locked = true;
        Data key = null;
        if (protocol.buffers != null && protocol.buffers.length > 0) {
            key = protocol.buffers[0];
        }
        DataMapProxy dataMapProxy = getMapProxy(name);
        if (timeout == 0) {
            locked = dataMapProxy.tryLock(key);
        } else {
            try {
                locked = dataMapProxy.tryLock(key, timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                locked = false;
            }
        }
        return protocol.success(String.valueOf(locked));
    }
}
