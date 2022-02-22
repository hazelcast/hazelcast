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

package com.hazelcast.wan.impl;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.wan.WanEvent;
import com.hazelcast.wan.WanEventCounters;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * Private API for WAN replication events. Adds methods for internal use by
 * our built-in WAN replication implementation.
 *
 * @param <T> type of object on which the event occurred
 */
public interface InternalWanEvent<T> extends WanEvent<T> {
    /**
     * Returns the key for the entry on which the event occurred.
     */
    @Nonnull
    Data getKey();

    /**
     * Returns the set of cluster names on which this event has already
     * been processed.
     */
    @Nonnull
    Set<String> getClusterNames();

    /**
     * Returns the backup count (sync and async) for this WAN event.
     */
    int getBackupCount();

    /**
     * Returns the creation time for this event in milliseconds.
     *
     * @see Clock#currentTimeMillis()
     */
    long getCreationTime();

    /**
     * Increments the count for the related event in the {@code counters}
     *
     * @param counters the WAN event counter
     */
    void incrementEventCount(@Nonnull WanEventCounters counters);
}
