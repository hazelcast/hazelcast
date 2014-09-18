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

package com.hazelcast.partition.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.MigrationInfo;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.ServiceInfo;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

public final class MigrationRequestOperation extends BaseMigrationOperation {

    private static final int TRY_PAUSE_MILLIS = 1000;

    private boolean returnResponse = true;

    public MigrationRequestOperation() {
    }

    public MigrationRequestOperation(MigrationInfo migrationInfo) {
        super(migrationInfo);
    }

    public void run() {
        NodeEngine nodeEngine = getNodeEngine();
        verifyGoodMaster(nodeEngine);

        Address source = migrationInfo.getSource();
        Address destination = migrationInfo.getDestination();
        verifyExistingTarget(nodeEngine, destination);

        if (destination.equals(source)) {
            getLogger().warning("Source and destination addresses are the same! => " + toString());
            success = false;
            return;
        }

        verifyNotThisNode(nodeEngine, source);

        InternalPartitionServiceImpl partitionService = getService();
        InternalPartition partition = partitionService.getPartition(migrationInfo.getPartitionId());
        Address owner = partition.getOwnerOrNull();
        verifyOwnerExists(owner);

        if (!migrationInfo.startProcessing()) {
            getLogger().warning("Migration is cancelled -> " + migrationInfo);
            success = false;
            return;
        }

        try {
            verifyOwner(source, partition, owner);
            partitionService.addActiveMigration(migrationInfo);
            long[] replicaVersions = partitionService.getPartitionReplicaVersions(migrationInfo.getPartitionId());
            Collection<Operation> tasks = prepareMigrationTasks();
            if (tasks.size() > 0) {
                invokeMigrationOperation(destination, replicaVersions, tasks);
                returnResponse = false;
            } else {
                success = true;
            }
        } catch (Throwable e) {
            logThrowable(e);
            success = false;
        } finally {
            migrationInfo.doneProcessing();
        }
    }

    private void logThrowable(Throwable t) {
        Throwable throwableToLog = t;
        if (throwableToLog instanceof ExecutionException) {
            throwableToLog = throwableToLog.getCause() != null ? throwableToLog.getCause() : throwableToLog;
        }
        Level level = getLogLevel(throwableToLog);
        getLogger().log(level, throwableToLog.getMessage(), throwableToLog);
    }

    private Level getLogLevel(Throwable e) {
        return (e instanceof MemberLeftException || e instanceof InterruptedException)
                || !getNodeEngine().isActive() ? Level.INFO : Level.WARNING;
    }

    private void verifyNotThisNode(NodeEngine nodeEngine, Address source) {
        if (source == null || !source.equals(nodeEngine.getThisAddress())) {
            throw new RetryableHazelcastException("Source of migration is not this node! => " + toString());
        }
    }

    private void verifyOwnerExists(Address owner) {
        if (owner == null) {
            throw new RetryableHazelcastException("Cannot migrate at the moment! Owner of the partition is null => "
                    + migrationInfo);
        }
    }

    private void verifyOwner(Address source, InternalPartition partition, Address owner) {
        if (!source.equals(owner)) {
            throw new HazelcastException("Cannot migrate! This node is not owner of the partition => "
                    + migrationInfo + " -> " + partition);
        }
    }

    private void invokeMigrationOperation(Address destination, long[] replicaVersions, Collection<Operation> tasks)
            throws IOException {

        MigrationOperation operation = new MigrationOperation(migrationInfo, replicaVersions, tasks);

        NodeEngine nodeEngine = getNodeEngine();
        nodeEngine.getOperationService()
                .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, operation, destination)
                .setCallback(new MigrationCallback(migrationInfo, getResponseHandler()))
                .setResultDeserialized(true)
                .setTryPauseMillis(TRY_PAUSE_MILLIS)
                .setReplicaIndex(getReplicaIndex())
                .invoke();
    }

    private void verifyGoodMaster(NodeEngine nodeEngine) {
        Address masterAddress = nodeEngine.getMasterAddress();
        if (!masterAddress.equals(migrationInfo.getMaster())) {
            throw new RetryableHazelcastException("Migration initiator is not master node! => " + toString());
        }
        if (!masterAddress.equals(getCallerAddress())) {
            throw new RetryableHazelcastException("Caller is not master node! => " + toString());
        }
    }

    private void verifyExistingTarget(NodeEngine nodeEngine, Address destination) {
        Member target = nodeEngine.getClusterService().getMember(destination);
        if (target == null) {
            throw new TargetNotMemberException("Destination of migration could not be found! => " + toString());
        }
    }

    @Override
    public ExceptionAction onException(Throwable throwable) {
        if (throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onException(throwable);
    }

    @Override
    public Object getResponse() {
        return success;
    }

    @Override
    public boolean returnsResponse() {
        return returnResponse;
    }

    private Collection<Operation> prepareMigrationTasks() {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        PartitionReplicationEvent replicationEvent = new PartitionReplicationEvent(migrationInfo.getPartitionId(), 0);
        PartitionMigrationEvent migrationEvent
                = new PartitionMigrationEvent(MigrationEndpoint.SOURCE, migrationInfo.getPartitionId());

        Collection<Operation> tasks = new LinkedList<Operation>();
        for (ServiceInfo serviceInfo : nodeEngine.getServiceInfos(MigrationAwareService.class)) {
            MigrationAwareService service = (MigrationAwareService) serviceInfo.getService();
            service.beforeMigration(migrationEvent);
            Operation op = service.prepareReplicationOperation(replicationEvent);
            if (op != null) {
                op.setServiceName(serviceInfo.getName());
                tasks.add(op);
            }
        }
        return tasks;
    }

    private static final class MigrationCallback implements Callback<Object> {

        final MigrationInfo migrationInfo;
        final ResponseHandler responseHandler;

        private MigrationCallback(MigrationInfo migrationInfo, ResponseHandler responseHandler) {
            this.migrationInfo = migrationInfo;
            this.responseHandler = responseHandler;
        }

        @Override
        public void notify(Object result) {
            migrationInfo.doneProcessing();
            responseHandler.sendResponse(result);
        }
    }
}
