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

package com.hazelcast.internal.jmx;

import com.hazelcast.internal.jmx.suppliers.StatsSupplier;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides LocalStats for {@link MapMBean}, {@link MultiMapMBean}, {@link QueueMBean}
 * So that statistics are only created in given internal
 * @param <T> LocalStats type
 */
public class LocalStatsDelegate<T> {

    private volatile T localStats;
    private final StatsSupplier<T> supplier;
    private final long intervalMs;
    private final AtomicLong lastUpdated = new AtomicLong(0);
    private final AtomicBoolean inProgress = new AtomicBoolean(false);

    public LocalStatsDelegate(StatsSupplier<T> supplier, long intervalSec) {
        this.supplier = supplier;
        this.intervalMs = TimeUnit.SECONDS.toMillis(intervalSec);
        this.localStats = supplier.getEmpty();
    }

    public T getLocalStats() {
        long delta = System.currentTimeMillis() - lastUpdated.get();
        if (delta > intervalMs) {
            if (inProgress.compareAndSet(false, true)) {
                try {
                    localStats = supplier.get();
                    lastUpdated.set(System.currentTimeMillis());
                } finally {
                    inProgress.set(false);
                }
            }
        }
        return localStats;
    }
}
