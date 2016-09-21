/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.NearCacheProvider;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventRegistration;

import java.util.Collection;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

/**
 * Sends invalidations to near-caches immediately.
 */
public class NonStopInvalidator extends AbstractNearCacheInvalidator {

    public NonStopInvalidator(MapServiceContext mapServiceContext, NearCacheProvider nearCacheProvider) {
        super(mapServiceContext, nearCacheProvider);
    }

    @Override
    public void invalidate(Data key, String mapName, String sourceUuid) {
        assert mapName != null;
        assert sourceUuid != null;

        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, mapName);
        for (EventRegistration registration : registrations) {
            if (canSendInvalidationEvent(registration, sourceUuid)) {
                SingleNearCacheInvalidation invalidation = new SingleNearCacheInvalidation(key, mapName, sourceUuid);
                eventService.publishEvent(SERVICE_NAME, registration, invalidation, orderKey(invalidation));
            }
        }
    }
}
