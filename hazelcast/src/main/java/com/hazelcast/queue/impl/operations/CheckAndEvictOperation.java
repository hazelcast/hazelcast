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

package com.hazelcast.queue.impl.operations;

import com.hazelcast.queue.impl.QueueContainer;
import com.hazelcast.queue.impl.QueueDataSerializerHook;
import com.hazelcast.spi.ProxyService;

/**
 * Provides eviction functionality for Operations of Queue.
 */
public class CheckAndEvictOperation extends QueueOperation {

    public CheckAndEvictOperation() {
    }

    public CheckAndEvictOperation(String name) {
        super(name);
    }

    public int getId() {
        return QueueDataSerializerHook.CHECK_EVICT;
    }

    @Override
    public void run() throws Exception {
        final QueueContainer container = getOrCreateContainer();
        if (container.isEvictable()) {
            ProxyService proxyService = getNodeEngine().getProxyService();
            proxyService.destroyDistributedObject(getServiceName(), name);
        }
    }
}
