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

package com.hazelcast.map.impl.query;

import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.LocalMapStatsProvider;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.spi.ExecutionService.QUERY_EXECUTOR;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.util.Collections.singletonList;

/**
 * Dispatches query operations.
 * Main responsibility is to:
 * - invoke proper query operation (QueryOperation or QueryPartitionOperation)
 * - invoke those operations on the proper member (local, remote, single, all, etc.)
 * <p>
 * Does not contain any query logic. Relies on query operations only.
 * Should be used by query engine only!
 */
final class QueryDispatcher {

    protected final MapServiceContext mapServiceContext;
    protected final NodeEngine nodeEngine;
    protected final ILogger logger;
    protected final InternalSerializationService serializationService;
    protected final IPartitionService partitionService;
    protected final OperationService operationService;
    protected final ClusterService clusterService;
    protected final LocalMapStatsProvider localMapStatsProvider;
    protected final ManagedExecutorService executor;

    protected QueryDispatcher(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        this.partitionService = nodeEngine.getPartitionService();
        this.logger = nodeEngine.getLogger(getClass());
        this.operationService = nodeEngine.getOperationService();
        this.clusterService = nodeEngine.getClusterService();
        this.localMapStatsProvider = mapServiceContext.getLocalMapStatsProvider();
        this.executor = nodeEngine.getExecutionService().getExecutor(QUERY_EXECUTOR);
    }

    protected List<Future<Result>> dispatchFullQueryOnQueryThread(
            Query query, Target target) {
        if (target.isTargetAllNodes()) {
            return dispatchFullQueryOnAllMembersOnQueryThread(query);
        } else if (target.isTargetLocalNode()) {
            return dispatchFullQueryOnLocalMemberOnQueryThread(query);
        }
        throw new IllegalArgumentException("Illegal target " + query);
    }

    private List<Future<Result>> dispatchFullQueryOnLocalMemberOnQueryThread(Query query) {
        Operation operation = new QueryOperation(query);
        Future<Result> result = operationService.invokeOnTarget(
                MapService.SERVICE_NAME, operation, nodeEngine.getThisAddress());
        return singletonList(result);
    }

    private List<Future<Result>> dispatchFullQueryOnAllMembersOnQueryThread(Query query) {
        Collection<Member> members = clusterService.getMembers(DATA_MEMBER_SELECTOR);
        List<Future<Result>> futures = new ArrayList<Future<Result>>(members.size());
        for (Member member : members) {
            Operation operation = new QueryOperation(query);
            Future<Result> future = operationService.invokeOnTarget(
                    MapService.SERVICE_NAME, operation, member.getAddress());
            futures.add(future);
        }
        return futures;
    }

    protected List<Future<Result>> dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(
            Query query, Collection<Integer> partitionIds) {
        if (shouldSkipPartitionsQuery(partitionIds)) {
            return Collections.emptyList();
        }

        List<Future<Result>> futures = new ArrayList<Future<Result>>(partitionIds.size());
        for (Integer partitionId : partitionIds) {
            futures.add(dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(query, partitionId));
        }
        return futures;
    }

    protected Future<Result> dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(Query query, int partitionId) {
        Operation op = new QueryPartitionOperation(query);
        op.setPartitionId(partitionId);
        try {
            return operationService.invokeOnPartition(MapService.SERVICE_NAME, op, partitionId);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private static boolean shouldSkipPartitionsQuery(Collection<Integer> partitionIds) {
        return partitionIds == null || partitionIds.isEmpty();
    }
}
