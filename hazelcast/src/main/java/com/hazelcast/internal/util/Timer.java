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

package com.hazelcast.internal.util;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Utility class to measure elapsed time in milliseconds. Backed by {@link System#nanoTime()}
 * Example usage {@code
 * long start = Timer.nanos();
 * ... // do some work
 * long elapsedNs = Timer.nanosElapsedSince(start);
 * long elapsedMs = Timer.millisElapsedSince(start);
 * }
 */
public final class Timer {
    private Timer() {
    }

    public static long nanos() {
        return System.nanoTime();
    }

    public static long nanosElapsed(long startNanos) {
        return System.nanoTime() - startNanos;
    }

    public static long microsElapsed(long startNanos) {
        return NANOSECONDS.toMicros(nanosElapsed(startNanos));
    }

    public static long millisElapsed(long startNano) {
        return NANOSECONDS.toMillis(nanosElapsed(startNano));
    }

    public static long secondsElapsed(long startNano) {
        return NANOSECONDS.toSeconds(nanosElapsed(startNano));
    }
}
