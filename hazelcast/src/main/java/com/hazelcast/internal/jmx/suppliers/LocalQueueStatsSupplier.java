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

package com.hazelcast.internal.jmx.suppliers;

import com.hazelcast.core.IQueue;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.monitor.impl.LocalQueueStatsImpl;

/**
 * Implementation of {@link StatsSupplier} for {@link LocalQueueStats}
 */
public class LocalQueueStatsSupplier implements StatsSupplier<LocalQueueStats> {

    private final IQueue queue;

    public LocalQueueStatsSupplier(IQueue queue) {
        this.queue = queue;
    }

    @Override
    public LocalQueueStats getEmpty() {
        return new LocalQueueStatsImpl();
    }

    @Override
    public LocalQueueStats get() {
        return queue.getLocalQueueStats();
    }
}
