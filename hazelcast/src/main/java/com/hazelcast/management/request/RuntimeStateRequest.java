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

package com.hazelcast.management.request;

import com.hazelcast.cluster.ClusterServiceImpl;
import com.hazelcast.concurrent.lock.LockResource;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.management.ClusterRuntimeState;
import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.InternalPartitionService;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author mdogan 5/7/12
 */
public class RuntimeStateRequest implements ConsoleRequest {

    private static final long LOCK_TIME_THRESHOLD = TimeUnit.SECONDS.toMillis(300);

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_CLUSTER_STATE;
    }

    private Collection<LockResource> collectLockState(final HazelcastInstanceImpl instance) {
        final LockService lockService = instance.node.nodeEngine.getService(LockService.SERVICE_NAME);
        return lockService.getAllLocks();
    }

    public void writeResponse(final ManagementCenterService mcs, final ObjectDataOutput dos) throws Exception {
        final HazelcastInstanceImpl instance = mcs.getHazelcastInstance();
        final ClusterServiceImpl cluster = instance.node.getClusterService();
        final InternalPartitionService partitionService = instance.node.partitionService;
        final Collection<LockResource> lockedRecords = collectLockState(instance);
        final Map<Address,Connection> connectionMap = instance.node.connectionManager.getReadonlyConnectionMap();

        final ClusterRuntimeState clusterRuntimeState = new ClusterRuntimeState(cluster.getMembers(), partitionService.getPartitions(),
                partitionService.getActiveMigrations(), connectionMap, lockedRecords);
        clusterRuntimeState.writeData(dos);
    }

    public Object readResponse(final ObjectDataInput in) throws IOException {
        ClusterRuntimeState clusterRuntimeState = new ClusterRuntimeState();
        clusterRuntimeState.readData(in);
        return clusterRuntimeState;
    }

    public void writeData(final ObjectDataOutput out) throws IOException {
    }

    public void readData(final ObjectDataInput in) throws IOException {
    }
}
