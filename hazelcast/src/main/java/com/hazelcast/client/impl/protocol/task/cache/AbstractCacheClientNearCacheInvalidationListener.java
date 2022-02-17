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

package com.hazelcast.client.impl.protocol.task.cache;

import com.hazelcast.cache.impl.CacheContext;
import com.hazelcast.cache.impl.CacheEventListener;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.internal.nearcache.impl.invalidation.AbstractBaseNearCacheInvalidationListener;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidation;
import com.hazelcast.internal.services.NotifiableEventListener;
import com.hazelcast.spi.impl.eventservice.EventRegistration;

import java.util.UUID;

abstract class AbstractCacheClientNearCacheInvalidationListener extends AbstractBaseNearCacheInvalidationListener
        implements CacheEventListener, NotifiableEventListener<CacheService> {

    private final ClientEndpoint endpoint;
    private final CacheContext cacheContext;

    AbstractCacheClientNearCacheInvalidationListener(ClientEndpoint endpoint,
                                                     CacheContext cacheContext,
                                                     UUID localMemberUuid,
                                                     long correlationId) {
        super(localMemberUuid, correlationId);

        this.endpoint = endpoint;
        this.cacheContext = cacheContext;
    }

    @Override
    public void handleEvent(Object eventObject) {
        if (!endpoint.isAlive() || !(eventObject instanceof Invalidation)) {
            return;
        }

        sendInvalidation(((Invalidation) eventObject));
    }

    @Override
    public void onRegister(CacheService cacheService, String serviceName,
                           String topic, EventRegistration registration) {
        cacheContext.increaseInvalidationListenerCount();
    }

    @Override
    public void onDeregister(CacheService cacheService, String serviceName,
                             String topic, EventRegistration registration) {
        cacheContext.decreaseInvalidationListenerCount();
    }
}
