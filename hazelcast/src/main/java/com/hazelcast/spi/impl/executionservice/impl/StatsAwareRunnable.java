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

package com.hazelcast.spi.impl.executionservice.impl;

import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.ExecutorStats;

public class StatsAwareRunnable implements Runnable {

    private final long creationTime = Clock.currentTimeMillis();
    private final Runnable runnable;
    private final String executorName;
    private final ExecutorStats offloadedExecutorStats;

    public StatsAwareRunnable(Runnable runnable, String executorName,
                              ExecutorStats offloadedExecutorStats) {
        this.runnable = runnable;
        this.executorName = executorName;
        this.offloadedExecutorStats = offloadedExecutorStats;

        offloadedExecutorStats.startPending(executorName);
    }

    @Override
    public void run() {
        long start = Clock.currentTimeMillis();
        offloadedExecutorStats.startExecution(executorName, start - creationTime);

        try {
            runnable.run();
        } finally {
            offloadedExecutorStats.finishExecution(executorName, Clock.currentTimeMillis() - start);
        }
    }
}
