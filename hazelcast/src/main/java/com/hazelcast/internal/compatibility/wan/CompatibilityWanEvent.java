/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.compatibility.wan;

import javax.annotation.Nonnull;

/**
 * Interface for compatibility (4.x) WAN replication messages
 *
 * @param <T> type of event data
 */
public interface CompatibilityWanEvent<T> {
    /**
     * Returns the service name on which this event occurred.
     *
     * @return the service name on which this event occurred.
     * @see com.hazelcast.map.impl.MapService#SERVICE_NAME
     * @see com.hazelcast.cache.impl.CacheService#SERVICE_NAME
     */
    @Nonnull
    String getServiceName();

    /**
     * Returns the name of the distributed object (map or cache) on which this
     * event occurred.
     *
     * @return the distributed object name
     */
    @Nonnull
    String getObjectName();
}
