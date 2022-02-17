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

package com.hazelcast.wan;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Interface for all WAN replication messages
 *
 * @param <T> type of event data
 */
public interface WanEvent<T> {
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

    /**
     * Returns the type of this event.
     *
     * @return the WAN event type
     */
    @Nonnull
    WanEventType getEventType();

    /**
     * Returns the event object/data.
     *
     * @return the WAN event object
     */
    @Nullable
    T getEventObject();
}
