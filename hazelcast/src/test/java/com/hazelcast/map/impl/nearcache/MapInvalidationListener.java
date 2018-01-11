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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.core.IMap;
import com.hazelcast.internal.nearcache.NearCacheInvalidationListener;
import com.hazelcast.internal.nearcache.impl.invalidation.BatchNearCacheInvalidation;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidation;
import com.hazelcast.map.impl.nearcache.invalidation.InvalidationListener;
import com.hazelcast.map.impl.proxy.NearCachedMapProxyImpl;

import java.util.concurrent.atomic.AtomicLong;

public class MapInvalidationListener implements NearCacheInvalidationListener, InvalidationListener {

    private final AtomicLong invalidationCount = new AtomicLong();

    @Override
    public long getInvalidationCount() {
        return invalidationCount.get();
    }

    @Override
    public void resetInvalidationCount() {
        invalidationCount.set(0);
    }

    @Override
    public void onInvalidate(Invalidation invalidation) {
        if (invalidation instanceof BatchNearCacheInvalidation) {
            BatchNearCacheInvalidation batch = ((BatchNearCacheInvalidation) invalidation);
            int batchInvalidationCount = batch.getInvalidations().size();
            invalidationCount.addAndGet(batchInvalidationCount);
        } else {
            invalidationCount.incrementAndGet();
        }
    }

    public static NearCacheInvalidationListener createInvalidationEventHandler(IMap memberMap) {
        InvalidationListener invalidationListener = new MapInvalidationListener();
        ((NearCachedMapProxyImpl) memberMap).addNearCacheInvalidationListener(invalidationListener);

        return (NearCacheInvalidationListener) invalidationListener;
    }
}
