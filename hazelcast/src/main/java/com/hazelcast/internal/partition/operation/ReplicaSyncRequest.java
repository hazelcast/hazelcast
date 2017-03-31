/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.internal.cluster.impl.Versions;
import com.hazelcast.internal.partition.InternalReplicaFragmentNamespace;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.PartitionReplicaVersionManager;
import com.hazelcast.internal.partition.ReplicaErrorLogger;
import com.hazelcast.internal.partition.impl.InternalPartitionImpl;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.partition.impl.PartitionStateManager;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.FragmentedMigrationAwareService;
import com.hazelcast.spi.ReplicaFragmentNamespace;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * The request sent from a replica to the partition owner to synchronize the replica data. The partition owner can send a
 * response to the replica to retry the sync operation when:
 * <ul>
 * <li>the replica sync is not allowed (because migrations are not allowed)</li>
 * <li>the operation was received by a node which is not the partition owner</li>
 * <li>the maximum number of parallel synchronizations has already been reached</li>
 * </ul>
 * An empty response can be sent if the current replica version is 0.
 */
public final class ReplicaSyncRequest extends AbstractPartitionOperation
        implements PartitionAwareOperation, MigrationCycleOperation, Versioned {

    private Collection<ReplicaFragmentNamespace> allNamespaces;

    public ReplicaSyncRequest() {
        allNamespaces = Collections.emptySet();
    }

    public ReplicaSyncRequest(int partitionId, Collection<ReplicaFragmentNamespace> namespaces, int replicaIndex) {
        this.allNamespaces = namespaces;
        setPartitionId(partitionId);
        setReplicaIndex(replicaIndex);
    }

    @Override
    public void beforeRun() throws Exception {
        int syncReplicaIndex = getReplicaIndex();
        if (syncReplicaIndex < 1 || syncReplicaIndex > InternalPartition.MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("Replica index " + syncReplicaIndex
                    + " should be in the range [1-" + InternalPartition.MAX_BACKUP_COUNT + "]");
        }
    }

    @Override
    public void run() throws Exception {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();

        if (!partitionService.isReplicaSyncAllowed()) {
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("Migration is paused! Cannot run replica sync -> " + toString());
            }
            sendRetryResponse();
            return;
        }

        if (!preCheckReplicaSync(nodeEngine, partitionId, replicaIndex)) {
            return;
        }

        try {
            Collection<Operation> operations = new ArrayList<Operation>();
            if (allNamespaces.isEmpty()) {
                // version 3.8
                createDefaultReplicationOperations(operations);
                sendOperations(operations, Collections.singleton(InternalReplicaFragmentNamespace.INSTANCE));
            } else {
                Map<String, Collection<ReplicaFragmentNamespace>> namespacesByService = groupNamespacesByService();
                if (namespacesByService.remove(InternalReplicaFragmentNamespace.INSTANCE.getServiceName()) != null) {
                    createNonFragmentedReplicationOperations(operations);
                    sendOperations(operations, Collections.singleton(InternalReplicaFragmentNamespace.INSTANCE));
                }

                for (Map.Entry<String, Collection<ReplicaFragmentNamespace>> entry : namespacesByService.entrySet()) {
                    String serviceName = entry.getKey();
                    for (ReplicaFragmentNamespace namespace : entry.getValue()) {
                        operations.clear();
                        Collection<ReplicaFragmentNamespace> ns = Collections.singleton(namespace);
                        createFragmentReplicationOperations(operations, serviceName, ns);
                        sendOperations(operations, ns);
                    }
                }
            }
        } finally {
            partitionService.getReplicaManager().releaseReplicaSyncPermit();
        }
    }

    private void sendOperations(Collection<Operation> operations, Collection<ReplicaFragmentNamespace> ns)
            throws Exception {
        if (operations.isEmpty()) {
            logNoReplicaDataFound(getPartitionId(), ns, getReplicaIndex());
            sendEmptyResponse(ns);
        } else {
            sendResponse(operations, ns);
        }
    }

    /** Checks if we can continue with the replication or not. Can send a retry or empty response to the replica in some cases */
    private boolean preCheckReplicaSync(NodeEngineImpl nodeEngine, int partitionId, int replicaIndex) throws IOException {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
        InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(partitionId);
        Address owner = partition.getOwnerOrNull();

        ILogger logger = getLogger();
        if (!nodeEngine.getThisAddress().equals(owner)) {
            if (logger.isFinestEnabled()) {
                logger.finest("Wrong target! " + toString() + " cannot be processed! Target should be: " + owner);
            }
            sendRetryResponse();
            return false;
        }

//        long[] replicaVersions = partitionService.getPartitionReplicaVersions(partitionId);
//        long currentVersion = replicaVersions[replicaIndex - 1];
//        if (currentVersion == 0) {
//            if (logger.isFinestEnabled()) {
//                logger.finest("Current replicaVersion=0, sending empty response for partitionId="
//                        + getPartitionId() + ", replicaIndex=" + getReplicaIndex() + ", replicaVersions="
//                        + Arrays.toString(replicaVersions));
//            }
//            sendEmptyResponse();
//            return false;
//        }

        if (!partitionService.getReplicaManager().tryToAcquireReplicaSyncPermit()) {
            if (logger.isFinestEnabled()) {
                logger.finest(
                        "Max parallel replication process limit exceeded! Could not run replica sync -> " + toString());
            }
            sendRetryResponse();
            return false;
        }
        return true;
    }

    private Map<String, Collection<ReplicaFragmentNamespace>> groupNamespacesByService() {
        Map<String, Collection<ReplicaFragmentNamespace>> map = new HashMap<String, Collection<ReplicaFragmentNamespace>>();
        for (ReplicaFragmentNamespace namespace : allNamespaces) {
            Collection<ReplicaFragmentNamespace> namespaces = map.get(namespace.getServiceName());
            if (namespaces == null) {
                namespaces = new HashSet<ReplicaFragmentNamespace>();
                map.put(namespace.getServiceName(), namespaces);
            }
            namespaces.add(namespace);
        }
        return map;
    }

    /** Send a response to the replica to retry the replica sync */
    private void sendRetryResponse() {
        NodeEngine nodeEngine = getNodeEngine();
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();

        ReplicaSyncRetryResponse response = new ReplicaSyncRetryResponse(allNamespaces);
        response.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        Address target = getCallerAddress();
        OperationService operationService = nodeEngine.getOperationService();
        operationService.send(response, target);
    }

    private void createDefaultReplicationOperations(Collection<Operation> operations) {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Collection<ServiceInfo> services = nodeEngine.getServiceInfos(MigrationAwareService.class);
        PartitionReplicationEvent event = new PartitionReplicationEvent(getPartitionId(), getReplicaIndex());
        for (ServiceInfo serviceInfo : services) {
            MigrationAwareService service = (MigrationAwareService) serviceInfo.getService();
            Operation op = service.prepareReplicationOperation(event);
            if (op != null) {
                op.setServiceName(serviceInfo.getName());
                operations.add(op);
            }
        }
    }

    private void createNonFragmentedReplicationOperations(Collection<Operation> operations) {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Collection<ServiceInfo> services = nodeEngine.getServiceInfos(MigrationAwareService.class);
        PartitionReplicationEvent event = new PartitionReplicationEvent(getPartitionId(), getReplicaIndex());

        for (ServiceInfo serviceInfo : services) {
            MigrationAwareService service = (MigrationAwareService) serviceInfo.getService();
            if (service instanceof FragmentedMigrationAwareService) {
                continue;
            }

            Operation op = service.prepareReplicationOperation(event);
            if (op != null) {
                op.setServiceName(serviceInfo.getName());
                operations.add(op);
            }
        }
    }

    private void createFragmentReplicationOperations(Collection<Operation> operations, String serviceName,
            Collection<ReplicaFragmentNamespace> ns) {

        PartitionReplicationEvent event = new PartitionReplicationEvent(getPartitionId(), getReplicaIndex());
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        FragmentedMigrationAwareService service = nodeEngine.getService(serviceName);
        Operation op = service.prepareReplicationOperation(event, ns);
        if (op != null) {
            op.setServiceName(serviceName);
            operations.add(op);
        }
    }

    /** Send a noop synchronization response to the caller replica
     */
    private void sendEmptyResponse(Collection<ReplicaFragmentNamespace> ns) throws IOException {
        sendResponse(null, ns);
    }

    /** Send a synchronization response to the caller replica containing the replication operations to be executed */
    private void sendResponse(Collection<Operation> operations, Collection<ReplicaFragmentNamespace> ns) throws IOException {
        NodeEngine nodeEngine = getNodeEngine();

        ReplicaSyncResponse syncResponse = createResponse(operations, ns);
        Address target = getCallerAddress();
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("Sending sync response to -> " + target + " for partitionId="
                    + getPartitionId() + ", replicaIndex=" + getReplicaIndex() + ", namespaces=" + ns);
        }
        OperationService operationService = nodeEngine.getOperationService();
        operationService.send(syncResponse, target);
    }

    private ReplicaSyncResponse createResponse(Collection<Operation> operations,
            Collection<ReplicaFragmentNamespace> namespaces) throws IOException {

        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();
        InternalPartitionService partitionService = getService();
        PartitionReplicaVersionManager versionManager = partitionService.getPartitionReplicaVersionManager();

        Map<ReplicaFragmentNamespace, long[]> versionMap = new HashMap<ReplicaFragmentNamespace, long[]>(namespaces.size());
        for (ReplicaFragmentNamespace ns : namespaces) {
            long[] versions = versionManager.getPartitionReplicaVersions(partitionId, ns);
            versionMap.put(ns, versions);
        }

        ReplicaSyncResponse syncResponse = new ReplicaSyncResponse(operations, versionMap);
        syncResponse.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        return syncResponse;
    }

    private void logNoReplicaDataFound(int partitionId, Collection<ReplicaFragmentNamespace> namespaces, int replicaIndex) {
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("No replica data is found for partitionId=" + partitionId + ", replicaIndex=" + replicaIndex
                + ", namespaces= " + namespaces);
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    public void logError(Throwable e) {
        ReplicaErrorLogger.log(e, getLogger());
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        if (out.getVersion().isGreaterOrEqual(Versions.V3_9)) {
            out.writeInt(allNamespaces.size());
            for (ReplicaFragmentNamespace namespace : allNamespaces) {
                out.writeObject(namespace);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        if (in.getVersion().isGreaterOrEqual(Versions.V3_9)) {
            int len = in.readInt();
            allNamespaces = new ArrayList<ReplicaFragmentNamespace>(len);
            for (int i = 0; i < len; i++) {
                ReplicaFragmentNamespace ns = in.readObject();
                allNamespaces.add(ns);
            }
        }
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.REPLICA_SYNC_REQUEST;
    }
}
