/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi.impl.listener;

import com.hazelcast.client.spi.ProxyManager;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectEvent;

public final class LazyDistributedObjectEvent extends DistributedObjectEvent {

    private ProxyManager proxyManager;

    /**
     * Constructs a DistributedObject Event.
     *
     * @param eventType         The event type as an enum {@link EventType} integer.
     * @param serviceName       The service name of the DistributedObject.
     * @param objectName        The name of the DistributedObject.
     * @param distributedObject The DistributedObject for the event.
     * @param proxyManager      The ProxyManager for lazily creating the proxy if not available on the client.
     */
    public LazyDistributedObjectEvent(EventType eventType, String serviceName, String objectName,
                                      DistributedObject distributedObject,
                                      ProxyManager proxyManager) {
        super(eventType, serviceName, objectName, distributedObject);
        this.proxyManager = proxyManager;
    }

    @Override
    public DistributedObject getDistributedObject() {
        distributedObject = super.getDistributedObject();
        if (distributedObject == null) {
            distributedObject = proxyManager.getOrCreateProxy(getServiceName(), (String) getObjectName());
        }
        return distributedObject;
    }

}
