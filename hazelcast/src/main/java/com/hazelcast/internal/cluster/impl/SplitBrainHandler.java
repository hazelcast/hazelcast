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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.internal.cluster.Joiner;
import com.hazelcast.instance.impl.Node;

import java.util.concurrent.atomic.AtomicBoolean;

final class SplitBrainHandler implements Runnable {

    private final Node node;
    private final AtomicBoolean inProgress = new AtomicBoolean(false);

    SplitBrainHandler(Node node) {
        this.node = node;
    }

    @Override
    public void run() {
        if (!shouldRun()) {
            return;
        }

        if (inProgress.compareAndSet(false, true)) {
            try {
                searchForOtherClusters();
            } finally {
                inProgress.set(false);
            }
        }
    }

    private boolean shouldRun() {
        ClusterServiceImpl clusterService = node.getClusterService();
        if (!clusterService.isJoined()) {
            return false;
        }

        if (!clusterService.isMaster()) {
            return false;
        }

        if (!node.isRunning()) {
            return false;
        }

        final ClusterJoinManager clusterJoinManager = clusterService.getClusterJoinManager();
        if (clusterJoinManager.isJoinInProgress()) {
            return false;
        }

        final ClusterState clusterState = clusterService.getClusterState();
        return clusterState.isJoinAllowed();
    }

    private void searchForOtherClusters() {
        Joiner joiner = node.getJoiner();
        if (joiner != null) {
            joiner.searchForOtherClusters();
        }
    }
}
