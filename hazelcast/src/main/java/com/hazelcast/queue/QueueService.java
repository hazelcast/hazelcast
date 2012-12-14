/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.queue;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.MigrationType;
import com.hazelcast.queue.proxy.DataQueueProxy;
import com.hazelcast.queue.proxy.ObjectQueueProxy;
import com.hazelcast.queue.proxy.QueueProxy;
import com.hazelcast.spi.*;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * User: ali
 * Date: 11/14/12
 * Time: 12:21 AM
 */
public class QueueService implements ManagedService, MigrationAwareService, MembershipAwareService, RemoteService {

    private NodeEngine nodeEngine;

    public static final String NAME = "hz:impl:queueService";

    private final ILogger logger;

    private final ConcurrentMap<String, QueueContainer> containerMap = new ConcurrentHashMap<String, QueueContainer>();
    private final ConcurrentMap<String, QueueProxy> proxies = new ConcurrentHashMap<String, QueueProxy>();

    public QueueService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(QueueService.class.getName());
    }

    public QueueContainer getContainer(final String name) {
        QueueContainer container = containerMap.get(name);
        if (container == null) {
            container = new QueueContainer(this, nodeEngine.getPartitionId(nodeEngine.toData(name)), nodeEngine.getConfig().getQueueConfig(name), name);
            QueueContainer existing = containerMap.putIfAbsent(name, container);
            if (existing != null) {
                container = existing;
            }
        }
        return container;
    }

    public void addContainer(String name, QueueContainer container) {
        containerMap.put(name, container);
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    public void destroy() {
    }

    public void beforeMigration(MigrationServiceEvent migrationServiceEvent) {
        // TODO: what if partition has transactions? what if not?
    }

    public Operation prepareMigrationOperation(MigrationServiceEvent event) {
        if (event.getPartitionId() < 0 || event.getPartitionId() >= nodeEngine.getPartitionCount()) {
            return null; // is it possible
        }
        Map<String, QueueContainer> migrationData = new HashMap<String, QueueContainer>();
        for (Entry<String, QueueContainer> entry : containerMap.entrySet()) {
            String name = entry.getKey();
            QueueContainer container = entry.getValue();
            if (container.partitionId == event.getPartitionId() && container.config.getTotalBackupCount() >= event.getReplicaIndex()) {
                migrationData.put(name, container);
            }
        }
        return new QueueMigrationOperation(migrationData, event.getPartitionId(), event.getReplicaIndex());
    }

    public void commitMigration(MigrationServiceEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE){
            if (event.getMigrationType() == MigrationType.MOVE || event.getMigrationType() == MigrationType.MOVE_COPY_BACK){
                cleanMigrationData(event.getPartitionId(), event.getCopyBackReplicaIndex());
            }
        }
    }

    public void rollbackMigration(MigrationServiceEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            cleanMigrationData(event.getPartitionId(), -1);
        }
    }

    private void cleanMigrationData(int partitionId, int copyBack) {
        Iterator<Entry<String, QueueContainer>> iterator = containerMap.entrySet().iterator();
        while (iterator.hasNext()) {
            QueueContainer container = iterator.next().getValue();
            if (container.partitionId == partitionId && (copyBack ==-1 || container.config.getTotalBackupCount() < copyBack)) {
                iterator.remove();
            }
        }
    }

    public void memberAdded(MemberImpl member) {
    }

    public void memberRemoved(MemberImpl member) {
    }

    public ServiceProxy getProxy(Object... params) {
        final String name = String.valueOf(params[0]);
        if (params.length > 1 && Boolean.TRUE.equals(params[1])) {
            return new DataQueueProxy(name, this, nodeEngine);
        }
        final QueueProxy proxy = new ObjectQueueProxy(name, this, nodeEngine);
        final QueueProxy currentProxy = proxies.putIfAbsent(name, proxy);
        return currentProxy != null ? currentProxy : proxy;
    }

    public Collection<ServiceProxy> getProxies() {
        return new HashSet<ServiceProxy>(proxies.values());
    }

}
