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

package com.hazelcast.internal.tpcengine;

import com.hazelcast.internal.tpcengine.logging.TpcLogger;
import com.hazelcast.internal.tpcengine.logging.TpcLoggerLocator;
import com.hazelcast.internal.tpcengine.util.EpochClock;
import com.hazelcast.internal.tpcengine.util.Promise;

final class DeadlineTask implements Runnable, Comparable<DeadlineTask> {
    protected final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());

    private final DeadlineScheduler deadlineScheduler;
    private final EpochClock epochClock;
    Promise promise;
    long deadlineNanos;
    Runnable cmd;
    long periodNanos = -1;
    long delayNanos = -1;

    SchedulingGroup schedGroup;

    DeadlineTask(EpochClock epochClock, DeadlineScheduler deadlineScheduler) {
        this.epochClock = epochClock;
        this.deadlineScheduler = deadlineScheduler;
    }

    @Override
    public void run() {
        if (cmd != null) {
            cmd.run();
        }

        if (periodNanos != -1 || delayNanos != -1) {
            if (periodNanos != -1) {
                deadlineNanos += periodNanos;
            } else {
                deadlineNanos = epochClock.nanoTime() + delayNanos;
            }

            if (deadlineNanos < 0) {
                deadlineNanos = Long.MAX_VALUE;
            }

            if (!deadlineScheduler.offer(this)) {
                logger.warning("Failed schedule task: " + this + " because there is no space in scheduledTaskQueue");
            }
        } else {
            if (promise != null) {
                promise.complete(null);
            }
        }
    }

    @Override
    public int compareTo(DeadlineTask that) {
        if (that.deadlineNanos == this.deadlineNanos) {
            return 0;
        }

        return this.deadlineNanos > that.deadlineNanos ? 1 : -1;
    }

    @Override
    public String toString() {
        return "ScheduledTask{"
                + "promise=" + promise
                + ", deadlineNanos=" + deadlineNanos
                + ", task=" + cmd
                + ", periodNanos=" + periodNanos
                + ", delayNanos=" + delayNanos
                + '}';
    }
}
