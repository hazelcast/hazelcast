package com.hazelcast.sql.impl.operation;

import com.hazelcast.sql.impl.QueryFragment;
import com.hazelcast.sql.impl.QueryFragmentDescriptor;
import com.hazelcast.sql.impl.QueryFragmentMapping;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryPlan;
import com.hazelcast.sql.impl.physical.PhysicalNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Factory to create query execute operations.
 */
public class QueryExecuteOperationFactory {
    /** Query plan. */
    private final QueryPlan plan;

    /** Arguments. */
    private final List<Object> args;

    /** Query ID. */
    private final QueryId queryId;

    /** Local member ID. */
    private final String localMemberId;

    /** Map from fragment position to the member where it should be executed. */
    private Map<QueryFragment, String> replicatedMappedMemberIds;

    public QueryExecuteOperationFactory(QueryPlan plan, List<Object> args, QueryId queryId, String localMemberId) {
        this.plan = plan;
        this.args = args;
        this.queryId = queryId;
        this.localMemberId = localMemberId;
    }

    /**
     * Create query execute operation for the given member.
     *
     * @param targetMemberId Target member ID.
     * @return Operation.
     */
    public QueryExecuteOperation create(String targetMemberId) {
        List<QueryFragment> fragments = plan.getFragments();

        List<QueryFragmentDescriptor> descriptors = new ArrayList<>(fragments.size());

        int baseDeploymentOffset = ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE / 2);

        for (QueryFragment fragment : fragments) {
            QueryFragmentMapping mapping = fragment.getMapping();

            PhysicalNode node;
            List<String> mappedMemberIds;

            switch (mapping) {
                case ROOT:
                    // Fragment is only deployed on local node.
                    node = targetMemberId.equals(localMemberId) ? fragment.getNode() : null;
                    mappedMemberIds = Collections.singletonList(localMemberId);

                    break;

                case DATA_MEMBERS:
                    // Fragment is only deployed on data node. Member IDs will be derived from partition mapping.
                    node = plan.getPartitionMap().containsKey(targetMemberId) ? fragment.getNode() : null;
                    mappedMemberIds = null;

                    break;

                default:
                    assert mapping == QueryFragmentMapping.REPLICATED;

                    String memberId = getMemberForReplicatedFragment(fragment);

                    node = targetMemberId.equals(memberId) ? fragment.getNode() : null;
                    mappedMemberIds = Collections.singletonList(memberId);
            }

            descriptors.add(new QueryFragmentDescriptor(
                node,
                mapping,
                mappedMemberIds,
                0 // At the moment we try to deploy all fragments to a single stripe for NUMA locality.
            ));
        }

        return new QueryExecuteOperation(
            queryId,
            plan.getPartitionMap(),
            descriptors,
            plan.getOutboundEdgeMap(),
            plan.getInboundEdgeMap(),
            args,
            baseDeploymentOffset
        );
    }

    /**
     * Get member ID for the replicated fragment.
     *
     * @param fragment Fragment.
     * @return Member ID.
     */
    private String getMemberForReplicatedFragment(QueryFragment fragment) {
        assert fragment.getMapping() == QueryFragmentMapping.REPLICATED;

        if (replicatedMappedMemberIds == null) {
            replicatedMappedMemberIds = new IdentityHashMap<>();
        }
        else {
            String res = replicatedMappedMemberIds.get(fragment);

            if (res != null) {
                return res;
            }
        }

        List<String> dataMemberIds = plan.getDataMemberIds();

        String res = dataMemberIds.get(ThreadLocalRandom.current().nextInt(dataMemberIds.size()));

        replicatedMappedMemberIds.put(fragment, res);

        return res;
    }
}
