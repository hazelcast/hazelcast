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

package com.hazelcast.map.impl.query;

import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.LocalMapStatsProvider;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.map.impl.query.MapQueryEngineUtils.shouldSkipPartitionsQuery;
import static com.hazelcast.spi.ExecutionService.QUERY_EXECUTOR;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.util.Collections.singletonList;

/**
 * Dispatches query operations.
 * Does not contain any query logic. Relies on query operations only.
 * Should be used by query engine only!
 */
final class MapQueryDispatcher {

    protected final MapServiceContext mapServiceContext;
    protected final NodeEngine nodeEngine;
    protected final ILogger logger;
    protected final InternalSerializationService serializationService;
    protected final IPartitionService partitionService;
    protected final OperationService operationService;
    protected final ClusterService clusterService;
    protected final LocalMapStatsProvider localMapStatsProvider;
    protected final ManagedExecutorService executor;

    protected MapQueryDispatcher(MapServiceContext mapServiceContext) {
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

    protected List<Future<QueryResult>> dispatchFullQueryOnQueryThread(
            String mapName, Predicate predicate, IterationType iterationType, DispatchTarget target) {
        switch (target) {
            case LOCAL_MEMBER:
                return dispatchFullQueryOnLocalMemberOnQueryThread(mapName, predicate, iterationType);
            case ALL_MEMBERS:
                return dispatchFullQueryOnAllMembersOnQueryThread(mapName, predicate, iterationType);
            default:
                throw new RuntimeException("Illegal dispatch target " + target);
        }
    }

    protected List<Future<QueryResult>> dispatchFullQueryOnLocalMemberOnQueryThread(
            String mapName, Predicate predicate, IterationType iterationType) {
        Operation operation = new QueryOperation(mapName, predicate, iterationType);
        Future<QueryResult> result = operationService.invokeOnTarget(
                MapService.SERVICE_NAME, operation, nodeEngine.getThisAddress());
        return singletonList(result);
    }

    protected List<Future<QueryResult>> dispatchFullQueryOnAllMembersOnQueryThread(
            String mapName, Predicate predicate, IterationType iterationType) {
        Collection<Member> members = clusterService.getMembers(DATA_MEMBER_SELECTOR);
        List<Future<QueryResult>> futures = new ArrayList<Future<QueryResult>>(members.size());
        for (Member member : members) {
            Operation operation = new QueryOperation(mapName, predicate, iterationType);
            Future<QueryResult> future = operationService.invokeOnTarget(
                    MapService.SERVICE_NAME, operation, member.getAddress());
            futures.add(future);
        }
        return futures;
    }

    protected List<Future<QueryResult>> dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(
            String mapName, Predicate predicate, Collection<Integer> partitionIds, IterationType iterationType) {
        if (shouldSkipPartitionsQuery(partitionIds)) {
            return Collections.emptyList();
        }

        List<Future<QueryResult>> futures = new ArrayList<Future<QueryResult>>(partitionIds.size());
        for (Integer partitionId : partitionIds) {
            futures.add(dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(mapName, predicate, partitionId, iterationType));
        }
        return futures;
    }

    protected Future<QueryResult> dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(
            String mapName, Predicate predicate, Integer partitionId, IterationType iterationType) {
        Operation op = new QueryPartitionOperation(mapName, predicate, iterationType);
        op.setPartitionId(partitionId);
        try {
            return operationService.invokeOnPartition(MapService.SERVICE_NAME, op, partitionId);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    enum DispatchTarget {
        LOCAL_MEMBER,
        ALL_MEMBERS
    }

}
