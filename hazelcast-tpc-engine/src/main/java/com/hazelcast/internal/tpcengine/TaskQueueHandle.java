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

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * A handle to a {@link TaskQueue}. A TaskQueue should not be directly
 * accessed and the interactions like destruction, changing priorities,
 * adding tasks etc, should be done through the TaskQueueHandle.
 */
@SuppressWarnings({"checkstyle:VisibilityModifier"})
public class TaskQueueHandle {
    // todo: the visibility should be reduced.
    public final TaskQueue queue;
    private final TaskQueueMetrics metrics;

    public TaskQueueHandle(TaskQueue queue, TaskQueueMetrics metrics) {
        this.queue = checkNotNull(queue, "queue");
        this.metrics = checkNotNull(metrics, "metrics");
    }

    /**
     * Returns the TaskQueueMetrics associated with the TaskQueue this
     * handle is referring to.
     *
     * @return the metrics.
     */
    public TaskQueueMetrics metrics() {
        return metrics;
    }

    @Override
    public String toString() {
        return queue.name;
    }
}
