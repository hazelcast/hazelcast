/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.util;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNegative;

/**
 * A {@link NanoClock} that doesn't use the {@link System#nanoTime()} for every call to
 * {@link #nanoTime()} to reduce cost, but it will cache the result of previous calls for
 * some iterations.
 * <p/>
 * This class is not thread-safe.
 */
public class CachedNanoClock implements NanoClock {
    private static final long START_TIME = System.nanoTime();

    private final int refreshPeriod;
    private int iteration;
    private long value;

    /**
     * Creates a new CachedNanoClock.
     *
     * @param refreshPeriod the period. When set to 0, the caching is disabled.
     * @throws IllegalArgumentException when refreshPeriod smaller than 0.
     */
    public CachedNanoClock(int refreshPeriod) {
        this.refreshPeriod = checkNotNegative(refreshPeriod, "refreshPeriod");
    }

    @Override
    public void update() {
        value = System.nanoTime() - START_TIME;
        iteration = 0;
    }

    @Override
    public long nanoTime() {
        if (iteration == refreshPeriod) {
            value = System.nanoTime() - START_TIME;
            iteration = 0;
        } else {
            iteration++;
        }
        return value;
    }
}
