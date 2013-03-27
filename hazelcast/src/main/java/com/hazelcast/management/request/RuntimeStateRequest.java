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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.management.ClusterRuntimeState;
import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.PartitionServiceImpl;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * @mdogan 5/7/12
 */
public class RuntimeStateRequest implements ConsoleRequest, Callable<ClusterRuntimeState>, HazelcastInstanceAware {

    private static final long LOCK_TIME_THRESHOLD = TimeUnit.SECONDS.toMillis(300);
    private transient HazelcastInstance hazelcastInstance;

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_CLUSTER_STATE;
    }

    public ClusterRuntimeState call() throws Exception {
        final HazelcastInstanceImpl factory = (HazelcastInstanceImpl) hazelcastInstance;
        return createState(factory);
    }

    private ClusterRuntimeState createState(final HazelcastInstanceImpl factory) {
        final ClusterServiceImpl cluster = factory.node.getClusterService();
        final PartitionServiceImpl pm = factory.node.partitionService;
        final Collection<com.hazelcast.concurrent.lock.LockInfo> lockedRecords = null;
//        final Collection<com.hazelcast.concurrent.lock.LockInfo> lockedRecords = collectLockState(factory);
        return new ClusterRuntimeState(cluster.getMembers(), pm.getPartitions(), null,
                factory.node.connectionManager.getReadonlyConnectionMap(), lockedRecords);
    }

//    private Collection<com.hazelcast.concurrent.lock.LockInfo> collectLockState(final HazelcastInstanceImpl instance) {
//
//        final Collection<com.hazelcast.concurrent.lock.LockInfo> lockedRecords = new LinkedList<com.hazelcast.concurrent.lock.LockInfo>();
//        final LockService lockService = instance.node.nodeEngine.getService(LockService.SERVICE_NAME);
//        for (LockStoreContainer lockStoreContainer : lockService.getLockontainers()) {
//            for (LockStoreImpl lockStore : lockStoreContainer.getLockStores()) {
//                    lockedRecords.addAll(lockStore.getLocks().values());
//            }
//
//        }
//        return lockedRecords;
//    }

    public void writeResponse(final ManagementCenterService mcs, final ObjectDataOutput dos) throws Exception {
        ClusterRuntimeState clusterRuntimeState = createState(mcs.getHazelcastInstance());
        clusterRuntimeState.writeData(dos);
    }

    public Object readResponse(final ObjectDataInput in) throws IOException {
        ClusterRuntimeState clusterRuntimeState = new ClusterRuntimeState();
        clusterRuntimeState.readData(in);
        return clusterRuntimeState;
    }

    public void setHazelcastInstance(final HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    public void writeData(final ObjectDataOutput out) throws IOException {
    }

    public void readData(final ObjectDataInput in) throws IOException {
    }
}
