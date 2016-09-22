/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.impl;

import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.mapreduce.JobPartitionState;
import com.hazelcast.mapreduce.PartitionIdAware;
import com.hazelcast.mapreduce.RemoteMapReduceException;
import com.hazelcast.mapreduce.impl.operation.NotifyRemoteExceptionOperation;
import com.hazelcast.mapreduce.impl.task.JobPartitionStateImpl;
import com.hazelcast.mapreduce.impl.task.JobProcessInformationImpl;
import com.hazelcast.mapreduce.impl.task.JobSupervisor;
import com.hazelcast.mapreduce.impl.task.JobTaskConfiguration;
import com.hazelcast.mapreduce.impl.task.MemberAssigningJobProcessInformationImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.mapreduce.JobPartitionState.State.MAPPING;
import static com.hazelcast.mapreduce.JobPartitionState.State.PROCESSED;
import static com.hazelcast.mapreduce.JobPartitionState.State.REDUCING;
import static com.hazelcast.mapreduce.JobPartitionState.State.WAITING;

/**
 * This utility class contains a few basic operations that are needed in multiple places
 */
public final class MapReduceUtil {

    private static final String EXECUTOR_NAME_PREFIX = "mapreduce::hz::";
    private static final String SERVICE_NAME = MapReduceService.SERVICE_NAME;

    private static final float DEFAULT_MAP_GROWTH_FACTOR = 0.75f;
    private static final int RETRY_PARTITION_TABLE_MILLIS = 100;
    private static final long PARTITION_READY_TIMEOUT = 10000;

    private MapReduceUtil() {
    }

    public static JobProcessInformationImpl createJobProcessInformation(JobTaskConfiguration configuration,
                                                                        JobSupervisor supervisor) {
        NodeEngine nodeEngine = configuration.getNodeEngine();
        if (configuration.getKeyValueSource() instanceof PartitionIdAware) {
            int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
            return new JobProcessInformationImpl(partitionCount, supervisor);
        } else {
            int partitionCount = nodeEngine.getClusterService().getSize(DATA_MEMBER_SELECTOR);
            return new MemberAssigningJobProcessInformationImpl(partitionCount, supervisor);
        }
    }

    public static void notifyRemoteException(JobSupervisor supervisor, Throwable throwable) {
        MapReduceService mapReduceService = supervisor.getMapReduceService();
        NodeEngine nodeEngine = mapReduceService.getNodeEngine();
        try {
            Address jobOwner = supervisor.getJobOwner();
            if (supervisor.isOwnerNode()) {
                supervisor.notifyRemoteException(jobOwner, throwable);
            } else {
                String name = supervisor.getConfiguration().getName();
                String jobId = supervisor.getConfiguration().getJobId();
                NotifyRemoteExceptionOperation operation = new NotifyRemoteExceptionOperation(name, jobId, throwable);

                OperationService os = nodeEngine.getOperationService();
                os.send(operation, jobOwner);
            }
        } catch (Exception e) {
            ILogger logger = nodeEngine.getLogger(MapReduceUtil.class);
            logger.warning("Could not notify remote map-reduce owner", e);
        }
    }

    public static JobPartitionState.State stateChange(Address owner, int partitionId, JobPartitionState.State currentState,
                                                      JobProcessInformationImpl processInformation,
                                                      JobTaskConfiguration configuration) {

        JobPartitionState[] partitionStates = processInformation.getPartitionStates();
        JobPartitionState partitionState = partitionStates[partitionId];

        // If not yet assigned we don't need to check owner and state
        JobPartitionState.State finalState = null;
        if (partitionState != null) {
            if (!owner.equals(partitionState.getOwner())) {
                return null;
            }
            if (partitionState.getState() != currentState) {
                return null;
            }

            if (currentState == MAPPING) {
                finalState = stateChangeMapping(partitionId, partitionState, processInformation, owner, configuration);
            } else if (currentState == REDUCING) {
                finalState = stateChangeReducing(partitionId, partitionState, processInformation, owner);
            }
        }

        if (currentState == WAITING) {
            if (compareAndSwapPartitionState(partitionId, partitionState, processInformation, owner, MAPPING)) {
                finalState = MAPPING;
            }
        }

        return finalState;
    }

    private static JobPartitionState.State stateChangeReducing(int partitionId, JobPartitionState oldPartitionState,
                                                               JobProcessInformationImpl processInformation, Address owner) {

        if (compareAndSwapPartitionState(partitionId, oldPartitionState, processInformation, owner, PROCESSED)) {
            return PROCESSED;
        }
        return null;
    }

    private static JobPartitionState.State stateChangeMapping(int partitionId, JobPartitionState oldPartitionState,
                                                              JobProcessInformationImpl processInformation, Address owner,
                                                              JobTaskConfiguration configuration) {
        JobPartitionState.State newState = PROCESSED;
        if (configuration.getReducerFactory() != null) {
            newState = REDUCING;
        }
        if (compareAndSwapPartitionState(partitionId, oldPartitionState, processInformation, owner, newState)) {
            return newState;
        }
        return null;
    }

    private static boolean compareAndSwapPartitionState(int partitionId, JobPartitionState oldPartitionState,
                                                        JobProcessInformationImpl processInformation, Address owner,
                                                        JobPartitionState.State newState) {
        JobPartitionState newPartitionState = new JobPartitionStateImpl(owner, newState);
        return processInformation.updatePartitionState(partitionId, oldPartitionState, newPartitionState);
    }

    public static <V> List<V> executeOperation(Collection<Member> members,
                                               OperationFactory operationFactory,
                                               MapReduceService mapReduceService,
                                               NodeEngine nodeEngine) {
        final OperationService operationService = nodeEngine.getOperationService();

        final List<InternalCompletableFuture<V>> futures = new ArrayList<InternalCompletableFuture<V>>();
        final List<V> results = new ArrayList<V>();

        final List<Exception> exceptions = new ArrayList<Exception>(members.size());
        for (Member member : members) {
            try {
                Operation operation = operationFactory.createOperation();
                if (nodeEngine.getThisAddress().equals(member.getAddress())) {
                    // Locally we can call the operation directly
                    operation.setNodeEngine(nodeEngine);
                    operation.setCallerUuid(nodeEngine.getLocalMember().getUuid());
                    operation.setService(mapReduceService);
                    operation.run();

                    V response = (V) operation.getResponse();
                    if (response != null) {
                        results.add(response);
                    }
                } else {
                    InvocationBuilder ib = operationService.createInvocationBuilder(SERVICE_NAME,
                                                                                    operation,
                                                                                    member.getAddress());
                    final InternalCompletableFuture<V> future = ib.invoke();
                    futures.add(future);
                }
            } catch (Exception e) {
                exceptions.add(e);
            }
        }


        for (InternalCompletableFuture<V> future : futures) {
            try {
                V response = future.join();
                if (response != null) {
                    results.add(response);
                }
            } catch (Exception e) {
                exceptions.add(e);
            }
        }


        if (exceptions.size() > 0) {
            throw new RemoteMapReduceException("Exception on mapreduce operation", exceptions);
        }

        return results;
    }

    public static <V> V executeOperation(Operation operation, Address address, MapReduceService mapReduceService,
                                         NodeEngine nodeEngine) {

        ClusterService cs = nodeEngine.getClusterService();
        OperationService os = nodeEngine.getOperationService();
        boolean returnsResponse = operation.returnsResponse();

        try {
            if (cs.getThisAddress().equals(address)) {
                // Locally we can call the operation directly
                operation.setNodeEngine(nodeEngine);
                operation.setCallerUuid(nodeEngine.getLocalMember().getUuid());
                operation.setService(mapReduceService);
                operation.run();

                if (returnsResponse) {
                    return (V) operation.getResponse();
                }
            } else {
                if (returnsResponse) {
                    InvocationBuilder ib = os.createInvocationBuilder(SERVICE_NAME, operation, address);
                    return (V) ib.invoke().get();
                } else {
                    os.send(operation, address);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static String buildExecutorName(String name) {
        return EXECUTOR_NAME_PREFIX + name;
    }

    public static int mapSize(final int sourceSize) {
        return sourceSize == 0 ? 0 : (int) (sourceSize / DEFAULT_MAP_GROWTH_FACTOR) + 1;
    }

    public static void enforcePartitionTableWarmup(MapReduceService mapReduceService) throws TimeoutException {
        IPartitionService partitionService = mapReduceService.getNodeEngine().getPartitionService();
        int partitionCount = partitionService.getPartitionCount();
        long startTime = Clock.currentTimeMillis();
        for (int p = 0; p < partitionCount; p++) {
            while (partitionService.getPartitionOwner(p) == null) {
                try {
                    Thread.sleep(RETRY_PARTITION_TABLE_MILLIS);
                } catch (Exception ignore) {
                    EmptyStatement.ignore(ignore);
                }

                if (Clock.currentTimeMillis() - startTime > PARTITION_READY_TIMEOUT) {
                    throw new TimeoutException("Partition get ready timeout reached!");
                }
            }
        }
    }
}
