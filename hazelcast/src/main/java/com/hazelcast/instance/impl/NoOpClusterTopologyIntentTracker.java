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

package com.hazelcast.instance.impl;

public class NoOpClusterTopologyIntentTracker implements ClusterTopologyIntentTracker {

    @Override
    public void update(int previousSpecifiedReplicas, int updatedSpecifiedReplicas,
                       int previousReadyReplicas, int updatedReadyReplicas,
                       int previousCurrentReplicas, int updatedCurrentReplicas) {
    }

    @Override
    public ClusterTopologyIntent getClusterTopologyIntent() {
        return ClusterTopologyIntent.NOT_IN_MANAGED_CONTEXT;
    }

    @Override
    public void initialize() {
    }

    @Override
    public void destroy() {
    }

    @Override
    public void initializeClusterTopologyIntent(ClusterTopologyIntent clusterTopologyIntent) {
    }

    @Override
    public void shutdownWithIntent(ClusterTopologyIntent clusterTopologyIntent) {
    }

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public int getCurrentSpecifiedReplicaCount() {
        return 0;
    }

    @Override
    public void onMembershipChange() {
    }
}
