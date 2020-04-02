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

package com.hazelcast.internal.util;

import java.util.concurrent.TimeUnit;

/**
 * Utility class to measure elapsed time in milliseconds. Backed by {@link System#nanoTime()}
 * Example usage {@code
 * Timer timer = Timer.getSystemTimer();
 * long start = timer.nanos();
 * ... // do some work
 * long elapsedNs = timer.nanosElapsedSince(start);
 * long elapsedMs = timer.millisElapsedSince(start);
 * }
 */
public class Timer {
    private static Timer SYSTEM_TIMER = create(System::nanoTime);

    public static Timer getSystemTimer() {
        return SYSTEM_TIMER;
    }

    static Timer create(NanoClock nanoClock) {
        return new Timer(nanoClock);
    }

    private NanoClock nanoClock;

    public Timer(NanoClock nanoClock) {
        this.nanoClock = nanoClock;
    }

    public long nanosElapsedSince(long startNanos) {
        return nanos() - startNanos;
    }

    public long nanos() {
        return nanoClock.nanoTime();
    }

    public long millisElapsedSince(long startNano) {
        return TimeUnit.NANOSECONDS.toMillis(nanosElapsedSince(startNano));
    }

    public interface NanoClock {
        long nanoTime();
    }
}
