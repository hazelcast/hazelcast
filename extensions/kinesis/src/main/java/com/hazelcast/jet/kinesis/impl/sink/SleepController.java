/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kinesis.impl.sink;

import java.util.ArrayList;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

class SleepController {

    /**
     * Minimum sleep duration after sends, while rate limiting is necessary.
     */
    private static final long MINIMUM_SLEEP_MS = 100L;

    /**
     * Maximum sleep duration after sends, while rate limiting is necessary.
     */
    private static final long MAXIMUM_SLEEP_MS = 10_000L;

    /**
     * The increment of increasing/decreasing the sleep duration while
     * adaptively searching for the best value.
     */
    private static final int SLEEP_INCREMENT = 25;

    /**
     * When adjusting the sleep duration we want to handle the effects in a
     * non-equal manner. If we set a sleep duration and then still get
     * messages rejected by the stream, we immediately increase the
     * duration.
     * <p>
     * On successful sends however we don't necessarily want to react
     * immediately. A success means that the sleep duration we have used is
     * good, so we should keep it and use it for following sends.
     * <p>
     * Keeping the last used sleep duration for ever is also not a good idea,
     * because the data flow might have decreased in the meantime and we
     * might be sleeping too much. So we want to slowly decrease, just
     * not as fast as we have increased it.
     * <p>
     * The relative ratio of these two speeds is what this constant
     * expresses.
     */
    private static final int DEGRADATION_VS_RECOVERY_SPEED_RATIO = 10;

    private final SlowRecoveryDegrader<Long> degrader = initSleepTracker();

    long markSuccess() {
        degrader.ok();
        return degrader.output();
    }

    long markFailure() {
        degrader.error();
        return degrader.output();
    }

    private static SlowRecoveryDegrader<Long> initSleepTracker() {
        List<Long> list = new ArrayList<>();

        //default sleep when there haven't been errors for a while
        list.add(0L);

        //increasing sleeps when errors keep repeating
        for (long millis = MINIMUM_SLEEP_MS; millis <= MAXIMUM_SLEEP_MS; millis += SLEEP_INCREMENT) {
            list.add(MILLISECONDS.toNanos(millis));
        }

        return new SlowRecoveryDegrader<>(DEGRADATION_VS_RECOVERY_SPEED_RATIO, list.toArray(new Long[0]));
    }

}
