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

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.wan.impl.DistributedServiceWanEventCounters;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * Interface for compatibility (4.x) WAN replication events.
 * For all intents and purposes, it is equivalent to
 * {@link com.hazelcast.wan.ReplicationEventObject}.
 *
 * @param <T> type of object on which the event occurred
 */
public interface CompatibilityInternalWanEvent<T> extends CompatibilityWanEvent<T> {
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
     * Increments the count for the related event in the {@code counters}
     *
     * @param counters the WAN event counter
     */
    void incrementEventCount(@Nonnull DistributedServiceWanEventCounters counters);
}
