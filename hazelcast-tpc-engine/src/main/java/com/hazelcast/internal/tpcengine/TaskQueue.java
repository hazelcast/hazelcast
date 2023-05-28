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

import java.util.Queue;

/**
 * Should the scheduler be tied to the task queue?
 */
public class TaskQueue {
    protected final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());

    public final String name;
    public final int shares;
    public final Queue<Object> queue;
    public Scheduler scheduler;

    public TaskQueue(String name, int shares, Queue queue, Scheduler scheduler) {
        this.name = name;
        this.shares = shares;
        this.queue = queue;
        this.scheduler = scheduler;
    }

    // todo: keep?
    public boolean offer(Object task) {
        return queue.offer(task);
    }

    // todo: keep?
    public boolean isEmpty() {
        return queue.isEmpty() && scheduler.queue().isEmpty();
    }

    public boolean process() {
        // todo: we are ignoring the scheduler
        // todo: we should prevent running the same task in the same cycle.
        for (int l = 0; l < shares; l++) {
            Object task = queue.poll();
            if (task == null) {
                // there are no more tasks
                break;
            } else if (task instanceof Runnable) {
                try {
                    ((Runnable) task).run();
                } catch (Exception e) {
                    logger.warning(e);
                }
            } else {
                try {
                    scheduler.schedule(task);
                } catch (Exception e) {
                    logger.warning(e);
                }
            }
        }

        return !queue.isEmpty();
    }
}
