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
 * long start = Timer.millis();
 * ... // do some work
 * long elapsedMs = Timer.elapsedSince(start);
 * }
 */
public class Timer {
    public static long millis() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    }

    public static long elapsedSince(long millis) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - millis;
    }
}
