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

import com.hazelcast.internal.tpcengine.util.BoundPriorityQueue;

import java.util.PriorityQueue;

public final class DeadlineScheduler {
    private long earliestDeadlineNanos = -1;
    private final PriorityQueue<DeadlineTask> runQueue;

    public DeadlineScheduler(int capacity) {
        this.runQueue = new BoundPriorityQueue<>(capacity);
    }

    public long earliestDeadlineNanos() {
        return earliestDeadlineNanos;
    }

    public boolean offer(DeadlineTask task) {
        return runQueue.offer(task);
    }

    public void tick(long nowNanos) {
        while (true) {
            DeadlineTask task = runQueue.peek();

            if (task == null) {
                return;
            }

            if (task.deadlineNanos > nowNanos) {
                // Task should not yet be executed.
                earliestDeadlineNanos = task.deadlineNanos;
                // we are done since all other tasks have a larger deadline.
                return;
            }

            // the task first needs to be removed from the run queue.
            runQueue.poll();
            earliestDeadlineNanos = -1;

            // offer the ScheduledTask to the task queue.
            task.taskGroup.offer(task);
        }
    }
}
