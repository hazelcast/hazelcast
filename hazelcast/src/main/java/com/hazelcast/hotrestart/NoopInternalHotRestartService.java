/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.hotrestart;

import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.nio.Address;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Empty implementation of InternalHotRestartService to avoid null checks. This will provide default behaviour when hot restart
 * is not available or not enabled.
 */
public class NoopInternalHotRestartService implements InternalHotRestartService {

    @Override
    public boolean triggerForceStart() {
        return false;
    }

    @Override
    public boolean triggerPartialStart() {
        return false;
    }

    @Override
    public boolean isMemberExcluded(Address memberAddress, String memberUuid) {
        return false;
    }

    @Override
    public Set<String> getExcludedMemberUuids() {
        return Collections.emptySet();
    }

    @Override
    public void notifyExcludedMember(Address memberAddress) {
    }

    @Override
    public void handleExcludedMemberUuids(Address sender, Set<String> excludedMemberUuids) {
    }

    @Override
    public ClusterHotRestartStatusDTO getCurrentClusterHotRestartStatus() {
        return new ClusterHotRestartStatusDTO();
    }

    @Override
    public void resetHotRestartData() {
    }

    @Override
    public void waitPartitionReplicaSyncOnCluster(long timeout, TimeUnit unit) {
    }
}
