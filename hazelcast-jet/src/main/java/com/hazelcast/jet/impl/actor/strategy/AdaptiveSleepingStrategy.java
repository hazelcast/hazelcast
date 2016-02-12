/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.actor.strategy;

import com.hazelcast.jet.api.actor.SleepingStrategy;

import java.util.concurrent.locks.LockSupport;

public class AdaptiveSleepingStrategy implements SleepingStrategy {
    private static final int MAX_POWER_OF_TWO = 10;

    private int power = 1;

    //CHECKSTYLE:OFF
    private static long getDurationNanos(int power) {
        return (1L << power) * 1000L;
    }
    //CHECKSTYLE:ON

    private long nextSleepingDurationNanos(boolean loaded) {
        if (loaded) {
            power = 1;
        } else {
            if (power < MAX_POWER_OF_TWO) {
                power++;
            }
        }

        return getDurationNanos(power);
    }

    @Override
    public void await(boolean loaded) {
        LockSupport.parkNanos(nextSleepingDurationNanos(loaded));
    }
}
