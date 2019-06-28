/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.config.QueryConfig;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
import com.hazelcast.sql.impl.physical.FragmentPreparePhysicalNodeVisitor;
import com.hazelcast.sql.impl.physical.PhysicalNode;
import com.hazelcast.sql.impl.physical.RootPhysicalNode;
import com.hazelcast.sql.impl.worker.control.ControlThreadPool;
import com.hazelcast.sql.impl.worker.control.ExecuteControlTask;
import com.hazelcast.sql.impl.worker.data.BatchDataTask;
import com.hazelcast.sql.impl.worker.data.DataThreadPool;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.Partition;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlService;
import com.hazelcast.util.collection.PartitionIdSet;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Proxy for SQL service. Backed by either Calcite-based or no-op implementation.
 */
// TODO: Implement cancel.
// TODO: Handle membership changes! (MembershipAwareService)
public class SqlServiceImpl implements SqlService, ManagedService {
    /** Calcite optimizer class name. */
    private static final String OPTIMIZER_CLASS = "com.hazelcast.sql.impl.calcite.SqlCalciteOptimizer";

    /** Node engine. */
    private final NodeEngine nodeEngine;

    /** Logger. */
    private final ILogger logger;

    /** Query optimizer. */
    private final SqlOptimizer optimizer;

    /** Lock to handle concurrent events. */
    private final ReentrantReadWriteLock busyLock = new ReentrantReadWriteLock();

    /** Whether the service is stopped. */
    // TODO: Implement node stop support.
    private boolean stopped;

    private ControlThreadPool controlThreadPool;
    private DataThreadPool dataThreadPool;

    public SqlServiceImpl(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;

        this.logger = nodeEngine.getLogger(QueueService.class);

        optimizer = createOptimizer(nodeEngine, logger);

        QueryConfig cfg = nodeEngine.getConfig().getQueryConfig();

        // TODO: Validate config values.
        dataThreadPool = new DataThreadPool(cfg.getDataThreadCount());
        controlThreadPool = new ControlThreadPool(this, nodeEngine, cfg.getControlThreadCount(), dataThreadPool);
    }

    @Override
    public SqlCursor query(String sql, Object... args) {
        // TODO: Implement plan cache.
        PhysicalPlan plan = optimizer.prepare(sql);

        List<Object> args0;

        if (args == null || args.length == 0)
            args0 = Collections.emptyList();
        else {
            args0 = new ArrayList<>(args.length);

            Collections.addAll(args0, args);
        }

        QueryHandle handle = execute0(plan, args0);

        return new SqlCursorImpl(handle);
    }

    /**
     * Internal query execution routine.
     *
     * @param plan Plan.
     * @param args Arguments.
     * @return Result.
     */
    private QueryHandle execute0(PhysicalPlan plan, List<Object> args) {
        if (args == null)
            args = Collections.emptyList();

        busyLock.readLock().lock();

        try {
            if (stopped)
                throw new IllegalStateException("Node is being stopped.");

            QueryId queryId = QueryId.create(nodeEngine.getLocalMember().getUuid());

            // TODO: Adjustable batch size!
            QueryResultConsumer consumer = new QueryResultConsumerImpl(1024);

            QueryExecuteOperation op = prepareLocalOperation(queryId, plan, args, consumer);

            // TODO: Again: race!
            for (Member member : nodeEngine.getClusterService().getMembers()) {
                // TODO: Execute local operation directly.
                // TODO: Execute remote operation without generic pool (PacketDispatcher?)

                nodeEngine.getOperationService().invokeOnTarget(SqlService.SERVICE_NAME, op, member.getAddress());
            }

            return new QueryHandle(queryId, consumer);
        }
        finally {
            busyLock.readLock().unlock();
        }
    }

    private QueryExecuteOperation prepareLocalOperation(QueryId queryId, PhysicalPlan plan, List<Object> args,
        QueryResultConsumer rootConsumer) {
        Map<String, PartitionIdSet> partitionMap = preparePartitionMapping();

        List<QueryFragment> fragments = new ArrayList<>(plan.getNodes().size()); // TODO: Null safery

        for (PhysicalNode node : plan.getNodes())
            fragments.add(fragmentFromNode(node));

        return new QueryExecuteOperation(queryId, partitionMap, fragments, args, rootConsumer);
    }

    private QueryFragment fragmentFromNode(PhysicalNode node) {
        FragmentPreparePhysicalNodeVisitor visitor = new FragmentPreparePhysicalNodeVisitor();

        node.visit(visitor);

        // TODO: Race wrt to partition mapping, and to other fragments. Should be collected only once.
        Collection<Member> members = node instanceof RootPhysicalNode ?
            Collections.singletonList(nodeEngine.getLocalMember()) :
            nodeEngine.getClusterService().getMembers(MemberSelectors.DATA_MEMBER_SELECTOR);

        TreeSet<String> memberIds = new TreeSet<>();

        for (Member member : members)
            memberIds.add(member.getUuid());

        // TODO: Support parallelism: https://github.com/hazelcast/hazelcast/issues/15229
        return new QueryFragment(
            node,
            visitor.getOutboundEdge(),
            visitor.getInboundEdges(),
            memberIds,
            1
        );
    }

    public void onQueryExecuteRequest(ExecuteControlTask task) {
        controlThreadPool.submit(task);
    }

    public void onQueryBatchRequest(BatchDataTask task) {
        if (task.isMapped())
            dataThreadPool.submit(task);
        else
            controlThreadPool.submit(task);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        dataThreadPool.start();
        controlThreadPool.start();
    }

    @Override
    public void reset() {
        shutdown(true);
    }

    @Override
    public void shutdown(boolean terminate) {
        dataThreadPool.shutdown();
        controlThreadPool.shutdown();
    }

    /**
     * Prepare current partition mapping.
     *
     * @return Partition mapping.
     */
    private Map<String, PartitionIdSet> preparePartitionMapping() {
        // TODO: There is a race between getting current partition distribution and getting the list of members
        // TODO: during Node -> Fragment mapping. Need to make it atomic somehow.

        // TODO: Avoid re-calc if there were no migrations in between.
        Collection<Partition> parts = nodeEngine.getHazelcastInstance().getPartitionService().getPartitions();

        int partCnt = parts.size();

        Map<String, PartitionIdSet> res = new HashMap<>();

        for (Partition part : parts) {
            String ownerId = part.getOwner().getUuid();

            res.computeIfAbsent(ownerId, (key) -> new PartitionIdSet(partCnt)).add(part.getPartitionId());
        }

        return res;
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    /**
     * Create either normal or no-op optimizer instance.
     *
     * @param nodeEngine Node engine.
     * @param logger Logger.
     * @return Optimizer.
     */
    @SuppressWarnings("unchecked")
    private static SqlOptimizer createOptimizer(NodeEngine nodeEngine, ILogger logger) {
        SqlOptimizer res;

        try {
            Class cls = Class.forName(OPTIMIZER_CLASS);

            Constructor<SqlOptimizer> ctor = cls.getConstructor(NodeEngine.class, ILogger.class);

            res = ctor.newInstance(nodeEngine, logger);
        }
        catch (ReflectiveOperationException e) {
            logger.info(OPTIMIZER_CLASS + " is not in the classpath, fallback to no-op implementation.");

            res = new SqlNoopOptimizer();
        }

        return res;
    }
}
