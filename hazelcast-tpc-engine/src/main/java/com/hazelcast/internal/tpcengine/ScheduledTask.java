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

final class ScheduledTask implements Runnable, Comparable<ScheduledTask> {

    final Eventloop eventloop;
    Promise promise;
    long deadlineNanos;
    Runnable task;
    long periodNanos = -1;
    long delayNanos = -1;

    ScheduledTask(Eventloop eventloop) {
        this.eventloop = eventloop;
    }

    @Override
    public void run() {
        if (task != null) {
            task.run();
        }

        if (periodNanos != -1 || delayNanos != -1) {
            if (periodNanos != -1) {
                deadlineNanos += periodNanos;
            } else {
                deadlineNanos = eventloop.nanoClock.nanoTime() + delayNanos;
            }

            if (deadlineNanos < 0) {
                deadlineNanos = Long.MAX_VALUE;
            }

            if (!eventloop.scheduledTaskQueue.offer(this)) {
                eventloop.logger.warning("Failed schedule task: " + this + " because there is no space in scheduledTaskQueue");
            }
        } else {
            if (promise != null) {
                promise.complete(null);
            }
        }
    }

    @Override
    public int compareTo(ScheduledTask that) {
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
                + ", task=" + task
                + ", periodNanos=" + periodNanos
                + ", delayNanos=" + delayNanos
                + '}';
    }
}
