/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.proxyservice.impl;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.DistributedObjectUtil;
import com.hazelcast.internal.util.executor.StripedRunnable;

import java.util.Collection;
import java.util.UUID;

final class ProxyEventProcessor implements StripedRunnable {

    private final Collection<DistributedObjectListener> listeners;
    private final DistributedObjectEvent.EventType type;
    private final String serviceName;
    private final String objectName;
    private final DistributedObject object;
    private final UUID source;

    ProxyEventProcessor(Collection<DistributedObjectListener> listeners, DistributedObjectEvent.EventType eventType,
                        String serviceName, String objectName, DistributedObject object, UUID source) {
        this.listeners = listeners;
        this.type = eventType;
        this.serviceName = serviceName;
        this.objectName = objectName;
        this.object = object;
        this.source = source;
    }

    @Override
    public void run() {
        DistributedObjectEvent event = new DistributedObjectEvent(type, serviceName, objectName, object, source);
        for (DistributedObjectListener listener : listeners) {
            switch (type) {
                case CREATED:
                    listener.distributedObjectCreated(event);
                    break;
                case DESTROYED:
                    listener.distributedObjectDestroyed(event);
                    break;
                default:
                    throw new IllegalStateException("Unrecognized EventType:" + type);
            }
        }
    }

    @Override
    public int getKey() {
        return DistributedObjectUtil.getName(object).hashCode();
    }
}
