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

package com.hazelcast.sql.impl.calcite;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryFragment;
import com.hazelcast.sql.impl.QueryFragmentMapping;
import com.hazelcast.sql.impl.QueryPlan;
import com.hazelcast.sql.impl.calcite.operators.HazelcastSqlOperatorTable;
import com.hazelcast.sql.impl.calcite.rel.physical.AggregatePhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.FilterPhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.MapIndexScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.MapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.MaterializedInputPhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.PhysicalRelVisitor;
import com.hazelcast.sql.impl.calcite.rel.physical.ProjectPhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.ReplicatedMapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.ReplicatedToDistributedPhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.RootPhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.SortPhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.exchange.BroadcastExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.exchange.SingletonSortMergeExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.exchange.UnicastExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.join.HashJoinPhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.join.NestedLoopJoinPhysicalRel;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.aggregate.AggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.AverageAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.CountAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.DistributedAverageAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.MinMaxAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.SumAggregateExpression;
import com.hazelcast.sql.impl.physical.AggregatePhysicalNode;
import com.hazelcast.sql.impl.physical.FilterPhysicalNode;
import com.hazelcast.sql.impl.physical.MapIndexScanPhysicalNode;
import com.hazelcast.sql.impl.physical.MapScanPhysicalNode;
import com.hazelcast.sql.impl.physical.MaterializedInputPhysicalNode;
import com.hazelcast.sql.impl.physical.PhysicalNode;
import com.hazelcast.sql.impl.physical.ProjectPhysicalNode;
import com.hazelcast.sql.impl.physical.ReplicatedMapScanPhysicalNode;
import com.hazelcast.sql.impl.physical.ReplicatedToPartitionedPhysicalNode;
import com.hazelcast.sql.impl.physical.RootPhysicalNode;
import com.hazelcast.sql.impl.physical.SortPhysicalNode;
import com.hazelcast.sql.impl.physical.hash.AllFieldsHashFunction;
import com.hazelcast.sql.impl.physical.hash.FieldHashFunction;
import com.hazelcast.sql.impl.physical.io.BroadcastSendPhysicalNode;
import com.hazelcast.sql.impl.physical.io.ReceivePhysicalNode;
import com.hazelcast.sql.impl.physical.io.ReceiveSortMergePhysicalNode;
import com.hazelcast.sql.impl.physical.io.UnicastSendPhysicalNode;
import com.hazelcast.sql.impl.physical.join.HashJoinPhysicalNode;
import com.hazelcast.sql.impl.physical.join.NestedLoopJoinPhysicalNode;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

 /**
 * Visitor which produces query plan.
 */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:classfanoutcomplexity"})
public class PlanCreatePhysicalRelVisitor implements PhysicalRelVisitor {
    /** Partition mapping. */
    private final Map<UUID, PartitionIdSet> partMap;

    /** Data member IDs. */
    private final List<UUID> dataMemberIds;

    /** Data member addresses. */
    private final List<Address> dataMemberAddresses;

    /** Prepared fragments. */
    private final List<QueryFragment> fragments = new ArrayList<>();

    /** Upstream nodes. Normally it is a one node, except of multi-source operations (e.g. joins, sets, subqueries). */
    private final Deque<PhysicalNode> upstreamNodes = new ArrayDeque<>();

    /** ID of current edge. */
    private int nextEdgeGenerator;

    public PlanCreatePhysicalRelVisitor(
        Map<UUID, PartitionIdSet> partMap,
        List<UUID> dataMemberIds,
        List<Address> dataMemberAddresses
    ) {
        this.partMap = partMap;
        this.dataMemberIds = dataMemberIds;
        this.dataMemberAddresses = dataMemberAddresses;
    }

    public QueryPlan getPlan() {
        Map<Integer, Integer> outboundEdgeMap = new HashMap<>();
        Map<Integer, Integer> inboundEdgeMap = new HashMap<>();

        for (int i = 0; i < fragments.size(); i++) {
            QueryFragment fragment = fragments.get(i);

            Integer outboundEdge = fragment.getOutboundEdge();

            if (outboundEdge != null) {
                outboundEdgeMap.put(outboundEdge, i);
            }

            if (fragment.getInboundEdges() != null) {
                for (Integer inboundEdge : fragment.getInboundEdges()) {
                    inboundEdgeMap.put(inboundEdge, i);
                }
            }
        }

        return new QueryPlan(
            partMap,
            dataMemberIds,
            dataMemberAddresses,
            fragments,
            outboundEdgeMap,
            inboundEdgeMap
        );
    }

    @Override
    public void onRoot(RootPhysicalRel root) {
        PhysicalNode upstreamNode = pollSingleUpstream();

        RootPhysicalNode rootNode = new RootPhysicalNode(
            upstreamNode
        );

        addFragment(rootNode, QueryFragmentMapping.ROOT);
    }

    @Override
    public void onMapScan(MapScanPhysicalRel rel) {
        MapScanPhysicalNode scanNode = new MapScanPhysicalNode(
            rel.getTableUnwrapped().getName(),
            rel.getTable().getRowType().getFieldNames(),
            rel.getProjects(),
            convertFilter(rel.getFilter())
        );

        pushUpstream(scanNode);
    }

     @Override
     public void onMapIndexScan(MapIndexScanPhysicalRel rel) {
         MapIndexScanPhysicalNode scanNode = new MapIndexScanPhysicalNode(
             rel.getTableUnwrapped().getName(),
             rel.getTable().getRowType().getFieldNames(),
             rel.getProjects(),
             rel.getIndex().getName(),
             rel.getIndexFilter(),
             convertFilter(rel.getRemainderFilter())
         );

         pushUpstream(scanNode);
     }

    @Override
    public void onReplicatedMapScan(ReplicatedMapScanPhysicalRel rel) {
        ReplicatedMapScanPhysicalNode scanNode = new ReplicatedMapScanPhysicalNode(
            rel.getTableUnwrapped().getName(),
            rel.getTable().getRowType().getFieldNames(),
            rel.getProjects(),
            convertFilter(rel.getFilter())
        );

        pushUpstream(scanNode);
    }

    @Override
    public void onSort(SortPhysicalRel sort) {
        PhysicalNode upstreamNode = pollSingleUpstream();

        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();

        List<Expression> expressions = new ArrayList<>(collations.size());
        List<Boolean> ascs = new ArrayList<>(collations.size());

        for (RelFieldCollation collation : collations) {
            RelFieldCollation.Direction direction = collation.getDirection();
            int idx = collation.getFieldIndex();

            expressions.add(new ColumnExpression(idx));
            ascs.add(!direction.isDescending());
        }

        SortPhysicalNode sortNode = new SortPhysicalNode(upstreamNode, expressions, ascs);

        pushUpstream(sortNode);
    }

    @Override
    public void onUnicastExchange(UnicastExchangePhysicalRel rel) {
        // Get upstream node.
        PhysicalNode upstreamNode = pollSingleUpstream();

        // Create sender and push it as a fragment.
        int edge = nextEdge();

        UnicastSendPhysicalNode sendNode = new UnicastSendPhysicalNode(
            upstreamNode,
            edge,
            new FieldHashFunction(rel.getHashFields())
        );

        addFragment(sendNode,  QueryFragmentMapping.DATA_MEMBERS);

        // Create receiver.
        ReceivePhysicalNode receiveNode = new ReceivePhysicalNode(edge);

        pushUpstream(receiveNode);
    }

    @Override
    public void onBroadcastExchange(BroadcastExchangePhysicalRel rel) {
        // Get upstream node.
        PhysicalNode upstreamNode = pollSingleUpstream();

        // Create sender and push it as a fragment.
        int edge = nextEdge();

        BroadcastSendPhysicalNode sendNode = new BroadcastSendPhysicalNode(
            upstreamNode,
            edge
        );

        addFragment(sendNode,  QueryFragmentMapping.DATA_MEMBERS);

        // Create receiver.
        ReceivePhysicalNode receiveNode = new ReceivePhysicalNode(edge);

        pushUpstream(receiveNode);
    }

    @Override
    public void onSingletonSortMergeExchange(SingletonSortMergeExchangePhysicalRel rel) {
        // Get upstream node. It should be sort node.
        PhysicalNode upstreamNode = pollSingleUpstream();

        assert upstreamNode instanceof SortPhysicalNode;

        SortPhysicalNode sortNode = (SortPhysicalNode) upstreamNode;

        // Create sender and push it as a fragment.
        int edge = nextEdge();

        UnicastSendPhysicalNode sendNode = new UnicastSendPhysicalNode(
            sortNode,
            edge,
            AllFieldsHashFunction.INSTANCE
        );

        addFragment(sendNode, QueryFragmentMapping.DATA_MEMBERS);

        // Create a receiver and push it to stack.
        ReceiveSortMergePhysicalNode receiveNode = new ReceiveSortMergePhysicalNode(
            edge,
            sortNode.getExpressions(),
            sortNode.getAscs()
        );

        pushUpstream(receiveNode);
    }

     @Override
     public void onReplicatedToDistributed(ReplicatedToDistributedPhysicalRel rel) {
         PhysicalNode upstreamNode = pollSingleUpstream();

         ReplicatedToPartitionedPhysicalNode replicatedToPartitionedNode = new ReplicatedToPartitionedPhysicalNode(
             upstreamNode,
             new FieldHashFunction(rel.getHashFields())
         );

         pushUpstream(replicatedToPartitionedNode);
     }

     @Override
    public void onProject(ProjectPhysicalRel rel) {
        PhysicalNode upstreamNode = pollSingleUpstream();

        List<RexNode> projects = rel.getProjects();
        List<Expression> convertedProjects = new ArrayList<>(projects.size());

        for (RexNode project : projects) {
            Expression convertedProject = project.accept(ExpressionConverterRexVisitor.INSTANCE);

            convertedProjects.add(convertedProject);
        }

        ProjectPhysicalNode projectNode = new ProjectPhysicalNode(upstreamNode, convertedProjects);

        pushUpstream(projectNode);
    }

    @Override
    public void onFilter(FilterPhysicalRel rel) {
        PhysicalNode upstreamNode = pollSingleUpstream();

        Expression<Boolean> filter = convertFilter(rel.getCondition());

        FilterPhysicalNode filterNode = new FilterPhysicalNode(upstreamNode, filter);

        pushUpstream(filterNode);
    }

    @Override
    public void onAggregate(AggregatePhysicalRel rel) {
        PhysicalNode upstreamNode = pollSingleUpstream();

        List<Integer> groupKey = rel.getGroupSet().toList();
        int sortedGroupKeySize = rel.getSortedGroupSet().toList().size();

        List<AggregateCall> aggCalls = rel.getAggCallList();

        List<AggregateExpression> aggAccumulators = new ArrayList<>();

        for (AggregateCall aggCall : aggCalls) {
            AggregateExpression aggAccumulator = convertAggregateCall(aggCall);

            aggAccumulators.add(aggAccumulator);
        }

        AggregatePhysicalNode aggNode = new AggregatePhysicalNode(
            upstreamNode,
            groupKey,
            aggAccumulators,
            sortedGroupKeySize
        );

        pushUpstream(aggNode);
    }

     @SuppressWarnings("unchecked")
     @Override
     public void onNestedLoopJoin(NestedLoopJoinPhysicalRel rel) {
         PhysicalNode leftInput = pollSingleUpstream();
         PhysicalNode rightInput = pollSingleUpstream();

         RexNode condition = rel.getCondition();
         Expression convertedCondition = condition.accept(ExpressionConverterRexVisitor.INSTANCE);

         NestedLoopJoinPhysicalNode joinNode = new NestedLoopJoinPhysicalNode(
             leftInput,
             rightInput,
             convertedCondition,
             rel.isOuter(),
             rel.isSemi(),
             rel.getRightRowColumnCount()
         );

         pushUpstream(joinNode);
     }

     @SuppressWarnings("unchecked")
     @Override
     public void onHashJoin(HashJoinPhysicalRel rel) {
         PhysicalNode leftInput = pollSingleUpstream();
         PhysicalNode rightInput = pollSingleUpstream();

         RexNode condition = rel.getCondition();
         Expression convertedCondition = condition.accept(ExpressionConverterRexVisitor.INSTANCE);

         HashJoinPhysicalNode joinNode = new HashJoinPhysicalNode(
             leftInput,
             rightInput,
             convertedCondition,
             rel.getLeftHashKeys(),
             rel.getRightHashKeys(),
             rel.isOuter(),
             rel.isSemi(),
             rel.getRightRowColumnCount()
         );

         pushUpstream(joinNode);
     }

     @Override
     public void onMaterializedInput(MaterializedInputPhysicalRel rel) {
         PhysicalNode input = pollSingleUpstream();

         MaterializedInputPhysicalNode node = new MaterializedInputPhysicalNode(input);

         pushUpstream(node);
     }

     public static AggregateExpression convertAggregateCall(AggregateCall aggCall) {
        SqlAggFunction aggFunc = aggCall.getAggregation();
        List<Integer> argList = aggCall.getArgList();

        boolean distinct = aggCall.isDistinct();

        switch (aggFunc.getKind()) {
            case SUM:
                return new SumAggregateExpression(distinct, new ColumnExpression<>(argList.get(0)));

            case COUNT:
                // TODO: COUNT(*) goes without aggregate! The whole row is the aggregate then!
                return new CountAggregateExpression(distinct, new ColumnExpression<>(argList.get(0)));

            case MIN:
                return new MinMaxAggregateExpression(true, distinct, new ColumnExpression<>(argList.get(0)));

            case MAX:
                return new MinMaxAggregateExpression(false, distinct, new ColumnExpression<>(argList.get(0)));

            case AVG:
                return new AverageAggregateExpression(distinct, new ColumnExpression<>(argList.get(0)));

            case OTHER_FUNCTION:
                assert aggFunc == HazelcastSqlOperatorTable.DISTRIBUTED_AVG;

                return new DistributedAverageAggregateExpression(
                    new ColumnExpression(argList.get(0)),
                    new ColumnExpression(argList.get(1))
                );

            default:
                throw new HazelcastSqlException(-1, "Unsupported aggregate call: " + aggFunc.getName());
        }
    }

    /**
     * Push node to upstream stack.
     *
     * @param node Node.
     */
    private void pushUpstream(PhysicalNode node) {
        upstreamNodes.addFirst(node);
    }

    /**
     * Poll an upstream node which is expected to be the only available in the stack.
     *
     * @return Upstream node.
     */
    private PhysicalNode pollSingleUpstream() {
        return upstreamNodes.pollFirst();
    }

    /**
     * Create new fragment and clear intermediate state.
     *
     * @param node Node.
     * @param mapping Fragment mapping mode.
     */
    private void addFragment(PhysicalNode node, QueryFragmentMapping mapping) {
        EdgeCollectorPhysicalNodeVisitor edgeVisitor = new EdgeCollectorPhysicalNodeVisitor();

        node.visit(edgeVisitor);

        Integer outboundEdge = edgeVisitor.getOutboundEdge();
        List<Integer> inboundEdges = edgeVisitor.getInboundEdges();

        QueryFragment fragment = new QueryFragment(
            node,
            outboundEdge,
            inboundEdges,
            mapping
        );

        fragments.add(fragment);
    }

    private int nextEdge() {
        return nextEdgeGenerator++;
    }

    @SuppressWarnings("unchecked")
    private static Expression<Boolean> convertFilter(RexNode expression) {
        if (expression == null) {
            return null;
        }

         Expression convertedExpression = expression.accept(ExpressionConverterRexVisitor.INSTANCE);

         return (Expression<Boolean>) convertedExpression;
     }
}
