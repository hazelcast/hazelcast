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

/**
 * A clock that returns the current time in nanoseconds from the epoch.
 * <p>
 * This class is thread-safe.
 */
public class EpochClock implements Clock {

    public static final EpochClock INSTANCE = new EpochClock();

    private static final long START_TIME = System.nanoTime();
//
//    static {
//        for(;;){
//            Instant instant = Instant.now();
//            long secondsSinceEpoch = instant.getEpochSecond();
//            int nanoSeconds = instant.getNano();
//            long nanos = nanoSeconds * TimeUnit.SECONDS.toNanos(secondsSinceEpoch);
//        }
//    }

    @Override
    public long nanoTime() {
        return System.nanoTime() - START_TIME;
    }

}
