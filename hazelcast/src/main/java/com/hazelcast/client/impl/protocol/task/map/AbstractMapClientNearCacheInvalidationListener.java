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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.internal.nearcache.impl.invalidation.AbstractBaseNearCacheInvalidationListener;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidation;
import com.hazelcast.map.impl.nearcache.invalidation.InvalidationListener;

import java.util.UUID;

/**
 * Invalidation listener abstraction helps to handle some different behaviour between clients in a backward compatible way.
 */
abstract class AbstractMapClientNearCacheInvalidationListener
        extends AbstractBaseNearCacheInvalidationListener
        implements InvalidationListener {

    private final ClientEndpoint endpoint;

    AbstractMapClientNearCacheInvalidationListener(ClientEndpoint endpoint,
                                                   UUID localMemberUuid, long correlationId) {
        super(localMemberUuid, correlationId);
        this.endpoint = endpoint;
    }

    @Override
    public void onInvalidate(Invalidation invalidation) {
        if (!endpoint.isAlive()) {
            return;
        }

        sendInvalidation(invalidation);
    }
}
