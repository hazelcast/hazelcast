/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.util.Clock;

public class SplitBrainHandler implements Runnable {
    final Node node;
    final ILogger logger;
    @SuppressWarnings("VolatileLongOrDoubleField")
    volatile long lastRun = 0;
    volatile boolean inProgress = false;
    final long FIRST_RUN_DELAY_MILLIS;
    final long NEXT_RUN_DELAY_MILLIS;

    public SplitBrainHandler(Node node) {
        this.node = node;
        this.logger = node.getLogger(SplitBrainHandler.class.getName());
        FIRST_RUN_DELAY_MILLIS = node.getGroupProperties().MERGE_FIRST_RUN_DELAY_SECONDS.getLong() * 1000L;
        NEXT_RUN_DELAY_MILLIS = node.getGroupProperties().MERGE_NEXT_RUN_DELAY_SECONDS.getLong() * 1000L;
        lastRun = Clock.currentTimeMillis() + FIRST_RUN_DELAY_MILLIS;
    }

    public void run() {
        if (node.isMaster() && node.joined() && node.isActive()) {
            long now = Clock.currentTimeMillis();
            if (!inProgress && (now - lastRun > NEXT_RUN_DELAY_MILLIS) && node.clusterImpl.shouldTryMerge()) {
                inProgress = true;
                node.executorManager.executeNow(new FallThroughRunnable() {
                    @Override
                    public void doRun() {
                        searchForOtherClusters();
                        lastRun = Clock.currentTimeMillis();
                        inProgress = false;
                    }
                });
            }
        }
    }

    private void searchForOtherClusters() {
        Joiner joiner = node.getJoiner();
        if (joiner != null) {
            joiner.searchForOtherClusters(this);
        }
    }

    public void restart() {
        lastRun = Clock.currentTimeMillis() + FIRST_RUN_DELAY_MILLIS;
        node.factory.restart();
    }
}
