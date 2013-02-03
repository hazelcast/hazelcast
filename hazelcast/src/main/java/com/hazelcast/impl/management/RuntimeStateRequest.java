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

package com.hazelcast.impl.management;

import com.hazelcast.cluster.ClusterImpl;
import com.hazelcast.core.*;
import com.hazelcast.impl.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
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
        final FactoryImpl factory = (FactoryImpl) hazelcastInstance;
        return createState(factory);
    }

    private ClusterRuntimeState createState(final FactoryImpl factory) {
        final ClusterImpl cluster = factory.getCluster();
        final PartitionManager pm = factory.node.concurrentMapManager.getPartitionManager();
        final Collection<Record> lockedRecords = collectLockState(factory);
        return new ClusterRuntimeState(cluster.getMembers(), pm.getPartitions(), pm.getMigratingPartition(),
                                  factory.node.connectionManager.getReadonlyConnectionMap(), lockedRecords);
    }

    private Collection<Record> collectLockState(final FactoryImpl factory) {
        final List<String> mapNames = new LinkedList<String>();
        mapNames.add(Prefix.LOCKS_MAP_HAZELCAST);
        for (Instance instance : factory.getInstances()) {
            if (instance.getInstanceType().isMap()) {
                IMap imap = (IMap) instance;
                mapNames.add(Prefix.MAP + imap.getName());
            }
        }
        final Node node = factory.node;
        final Collection<Record> lockedRecords = new LinkedList<Record>();
        for (final String mapName : mapNames) {
            final CMap cmap = node.concurrentMapManager.getMap(mapName);
            if (cmap != null) {
                Collection<Record> records = cmap.getLockedRecordsFor(LOCK_TIME_THRESHOLD);
                lockedRecords.addAll(records);
            }
        }
        return lockedRecords;
    }

    public void writeResponse(final ManagementCenterService mcs, final DataOutput dos) throws Exception {
        ClusterRuntimeState clusterRuntimeState = createState(mcs.getHazelcastInstance());
        clusterRuntimeState.writeData(dos);
    }

    public Object readResponse(final DataInput in) throws IOException {
        ClusterRuntimeState clusterRuntimeState = new ClusterRuntimeState();
        clusterRuntimeState.readData(in);
        return clusterRuntimeState;
    }

    public void setHazelcastInstance(final HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    public void writeData(final DataOutput out) throws IOException {
    }

    public void readData(final DataInput in) throws IOException {
    }
}
