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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.internal.nearcache.impl.invalidation.Invalidation;
import com.hazelcast.map.listener.MapListener;

/**
 * Used to receive invalidation events from an {@link com.hazelcast.core.IMap IMap}
 * <p/>
 * For example, a client Near Cache implementation can listen changes in IMap data and
 * can remove stale data in its own cache.
 *
 * @since 3.6
 */
public interface InvalidationListener extends MapListener {

    /**
     * Called upon an invalidation.
     *
     * @param event the received {@link Invalidation} event.
     */
    void onInvalidate(Invalidation event);
}
