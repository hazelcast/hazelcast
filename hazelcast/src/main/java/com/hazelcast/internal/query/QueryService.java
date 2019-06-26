package com.hazelcast.internal.query;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.config.QueryConfig;
import com.hazelcast.internal.query.exec.RootConsumer;
import com.hazelcast.internal.query.operation.QueryExecuteOperation;
import com.hazelcast.internal.query.plan.physical.FragmentPrepareVisitor;
import com.hazelcast.internal.query.plan.physical.PhysicalNode;
import com.hazelcast.internal.query.plan.physical.PhysicalPlan;
import com.hazelcast.internal.query.plan.physical.RootPhysicalNode;
import com.hazelcast.internal.query.worker.control.ControlThreadPool;
import com.hazelcast.internal.query.worker.control.ExecuteControlTask;
import com.hazelcast.internal.query.worker.data.BatchDataTask;
import com.hazelcast.internal.query.worker.data.DataThreadPool;
import com.hazelcast.partition.Partition;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.collection.PartitionIdSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Service responsible for query execution.
 */
public class QueryService implements ManagedService, MembershipAwareService {
    /** Unique service name. */
    public static final String SERVICE_NAME = "hz:impl:QueryService";

    /** Node engine. */
    private NodeEngine nodeEngine;

    /** Active queries. */
    private ConcurrentHashMap<QueryId, QueryHandleImpl> activeQueries = new ConcurrentHashMap<>();

    /** Lock to handle concurrent events. */
    private final ReentrantReadWriteLock busyLock = new ReentrantReadWriteLock();

    /** Whether the service is stopped. */
    private boolean stopped;

    private ControlThreadPool controlThreadPool;
    private DataThreadPool dataThreadPool;

    public QueryHandle execute(PhysicalPlan plan, List<Object> args) {
        if (args == null)
            args = Collections.emptyList();

        busyLock.readLock().lock();

        try {
            if (stopped)
                throw new IllegalStateException("Node is being stopped.");

            // TODO: Adjustable batch size!
            RootConsumer consumer = new QueryRootConsumer(1024);

            QueryExecuteOperation op = prepareLocalOperation(plan, args, consumer);

            // TODO: Again: race!
            for (Member member : nodeEngine.getClusterService().getMembers()) {
                // TODO: Execute local operation directly.
                // TODO: Execute remote operation without generic pool (PacketDispatcher?)

                nodeEngine.getOperationService().invokeOnTarget(QueryService.SERVICE_NAME, op, member.getAddress());
            }

            return new QueryHandleImpl(this, op.getQueryId(), consumer);
        }
        finally {
            busyLock.readLock().unlock();
        }
    }

    private QueryExecuteOperation prepareLocalOperation(PhysicalPlan plan, List<Object> args,
        RootConsumer rootConsumer) {
        QueryId queryId = prepareQueryId();
        Map<String, PartitionIdSet> partitionMap = preparePartitionMapping();

        List<QueryFragment> fragments = new ArrayList<>(plan.getNodes().size()); // TODO: Null safery

        for (PhysicalNode node : plan.getNodes())
            fragments.add(fragmentFromNode(node));

        return new QueryExecuteOperation(queryId, partitionMap, fragments, args, rootConsumer);
    }

    private QueryFragment fragmentFromNode(PhysicalNode node) {
        FragmentPrepareVisitor visitor = new FragmentPrepareVisitor();

        node.visit(visitor);

        // TODO: Race wrt to partition mapping, and to other fragments. Should be collected only once.
        Collection<Member> members = node instanceof RootPhysicalNode ?
            Collections.singletonList(nodeEngine.getLocalMember()) :
            nodeEngine.getClusterService().getMembers(MemberSelectors.DATA_MEMBER_SELECTOR);

        TreeSet<String> memberIds = new TreeSet<>();

        for (Member member : members)
            memberIds.add(member.getUuid());

        int parallelism = visitor.getParallelism();

        if (parallelism <= 0)
            parallelism = 1;

        return new QueryFragment(
            node,
            visitor.getOutboundEdge(),
            visitor.getInboundEdges(),
            memberIds,
            parallelism
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

    public void onQueryCancelRequest(QueryId queryId, QueryCancelReason reason, String errMsg) {
        QueryHandleImpl handle = activeQueries.get(queryId);

        if (handle != null)
            handle.cancel(reason, errMsg);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;

        QueryConfig cfg = nodeEngine.getConfig().getQueryConfig();

        // TODO: Validate config values.
        dataThreadPool = new DataThreadPool(cfg.getDataThreadCount());
        controlThreadPool = new ControlThreadPool(this, cfg.getControlThreadCount(), dataThreadPool);

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

    @Override
    public void memberAdded(MembershipServiceEvent event) {
        // TODO
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        // TODO
    }

    @Override
    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
        // No-op.
    }

    private QueryId prepareQueryId() {
        String memberId = nodeEngine.getLocalMember().getUuid();
        UUID qryId = UUID.randomUUID();

        return new QueryId(memberId, qryId.getLeastSignificantBits(), qryId.getLeastSignificantBits());
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
}
