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

package com.hazelcast.partition;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.*;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public final class MigrationRequestOperation extends BaseMigrationOperation {

    private final static long serialVersionUID = 1;

    private transient boolean returnResponse = true;

    public MigrationRequestOperation() {
    }

    public MigrationRequestOperation(MigrationInfo migrationInfo) {
        super(migrationInfo);
    }

    public void run() {
        final NodeEngine nodeEngine = getNodeEngine();
        final Address masterAddress = nodeEngine.getMasterAddress();
        if (!masterAddress.equals(migrationInfo.getMaster())) {
            throw new RetryableHazelcastException("Migration initiator is not master node! => " + toString());
        }
        if (!masterAddress.equals(getCallerAddress())) {
            throw new RetryableHazelcastException("Caller is not master node! => " + toString());
        }

        final Address source = migrationInfo.getSource();
        final Address destination = migrationInfo.getDestination();
        final Member target = nodeEngine.getClusterService().getMember(destination);
        if (target == null) {
            throw new TargetNotMemberException("Destination of migration could not be found! => " + toString());
        }
        if (destination.equals(source)) {
            getLogger().warning("Source and destination addresses are the same! => " + toString());
            success = false;
            return;
        }

        if (source == null || !source.equals(nodeEngine.getThisAddress())) {
            throw new RetryableHazelcastException("Source of migration is not this node! => " + toString());
        }

        PartitionServiceImpl partitionService = getService();
        InternalPartition partition = partitionService.getPartition(migrationInfo.getPartitionId());
        final Address owner = partition.getOwner();
        if (owner == null) {
            throw new RetryableHazelcastException("Cannot migrate at the moment! Owner of the partition is null => "
                    + migrationInfo);
        }

        if (migrationInfo.startProcessing()) {
            try {
                if (!source.equals(owner)) {
                    throw new HazelcastException("Cannot migrate! This node is not owner of the partition => "
                            + migrationInfo + " -> " + partition);
                }
                partitionService.addActiveMigration(migrationInfo);
                final long[] replicaVersions = partitionService.getPartitionReplicaVersions(migrationInfo.getPartitionId());
                final long timeout = nodeEngine.getGroupProperties().PARTITION_MIGRATION_TIMEOUT.getLong();
                final Collection<Operation> tasks = prepareMigrationTasks();
                if (tasks.size() > 0) {
                    returnResponse = false;
                    final ResponseHandler responseHandler = getResponseHandler();
                    final SerializationService serializationService = nodeEngine.getSerializationService();

                    nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR).execute(new Runnable() {
                        public void run() {
                            final BufferObjectDataOutput out = serializationService.createObjectDataOutput(1024 * 32);
                            try {
                                out.writeInt(tasks.size());
                                for (Operation task : tasks) {
                                    serializationService.writeObject(out, task);
                                }
                                final byte[] data;
                                boolean compress = nodeEngine.getGroupProperties().PARTITION_MIGRATION_ZIP_ENABLED.getBoolean();
                                if (compress) {
                                    data = IOUtil.compress(out.toByteArray());
                                } else {
                                    data = out.toByteArray();
                                }
                                final MigrationOperation migrationOperation = new MigrationOperation(migrationInfo, replicaVersions, data, tasks.size(), compress);
                                Future future = nodeEngine.getOperationService().createInvocationBuilder(PartitionServiceImpl.SERVICE_NAME,
                                        migrationOperation, destination).setTryPauseMillis(1000).setReplicaIndex(getReplicaIndex()).invoke();
                                Boolean result = (Boolean) nodeEngine.toObject(future.get(timeout, TimeUnit.SECONDS));
                                migrationInfo.doneProcessing();
                                responseHandler.sendResponse(result);
                            } catch (Throwable e) {
                                responseHandler.sendResponse(Boolean.FALSE);
                                if (e instanceof ExecutionException) {
                                    e = e.getCause() != null ? e.getCause() : e;
                                }
                                Level level = (e instanceof MemberLeftException || e instanceof InterruptedException)
                                        || !getNodeEngine().isActive() ? Level.INFO : Level.WARNING;
                                getLogger().log(level, e.getMessage(), e);
                            } finally {
                                IOUtil.closeResource(out);
                            }
                        }
                    });
                } else {
                    success = true;
                }
            } catch (Throwable e) {
                getLogger().warning( e);
                success = false;
            } finally {
//                if (returnResponse) {
                    migrationInfo.doneProcessing();
//                }
            }
        } else {
            getLogger().warning("Migration is cancelled -> " + migrationInfo);
            success = false;
        }
    }


    @Override
    public ExceptionAction onException(Throwable throwable) {
        if (throwable instanceof TargetNotMemberException) {
            NodeEngine nodeEngine = getNodeEngine();
            if (nodeEngine != null && nodeEngine.getClusterService().getMember(migrationInfo.getDestination()) == null) {
                return ExceptionAction.THROW_EXCEPTION;
            }
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
        final PartitionReplicationEvent replicationEvent = new PartitionReplicationEvent(migrationInfo.getPartitionId(), 0);
        final PartitionMigrationEvent migrationEvent = new PartitionMigrationEvent(MigrationEndpoint.SOURCE, migrationInfo.getPartitionId());
        final Collection<Operation> tasks = new LinkedList<Operation>();
        for (ServiceInfo serviceInfo : nodeEngine.getServiceInfos(MigrationAwareService.class)) {
            MigrationAwareService service = (MigrationAwareService) serviceInfo.getService();
            service.beforeMigration(migrationEvent);
            final Operation op = service.prepareReplicationOperation(replicationEvent);
            if (op != null) {
                op.setServiceName(serviceInfo.getName());
                tasks.add(op);
            }
        }
        return tasks;
    }
}
