/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import java.util.logging.Level;

public class SplitBrainHandler implements Processable {
    private final Node node;
    private final ILogger logger;
    private volatile boolean inProgress = false;
    private final long firstRunDelayMillis;
    private final long nextRunDelayMillis;

    public SplitBrainHandler(Node node) {
        this.node = node;
        this.logger = node.getLogger(SplitBrainHandler.class.getName());
        firstRunDelayMillis = node.getGroupProperties().MERGE_FIRST_RUN_DELAY_SECONDS.getLong() * 1000L;
        nextRunDelayMillis = node.getGroupProperties().MERGE_NEXT_RUN_DELAY_SECONDS.getLong() * 1000L;
    }

    public void process() {
        if (node.isMaster() && node.joined() && node.isActive()) {
            if (!inProgress && node.clusterManager.shouldTryMerge()) {
                inProgress = true;
                node.executorManager.executeNow(new FallThroughRunnable() {
                    @Override
                    public void doRun() {
                        searchForOtherClusters();
                        inProgress = false;
                    }
                });
            }
        }
    }

    private void searchForOtherClusters() {
        Joiner joiner = node.getJoiner();
        if (joiner != null) {
            logger.log(Level.FINEST, "Searching for other clusters.");
            node.getSystemLogService().logJoin("Searching for other clusters.");
            joiner.searchForOtherClusters(this);
        }
    }

    public void restart() {
        node.factory.restartToMerge();
    }

    public long getFirstRunDelayMillis() {
        return firstRunDelayMillis;
    }

    public long getNextRunDelayMillis() {
        return nextRunDelayMillis;
    }
}
