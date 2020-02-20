/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.opt.physical.visitor;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.OptimizerStatistics;
import com.hazelcast.sql.impl.QueryExplain;
import com.hazelcast.sql.impl.QueryFragment;
import com.hazelcast.sql.impl.QueryFragmentMapping;
import com.hazelcast.sql.impl.QueryPlan;
import com.hazelcast.sql.impl.calcite.EdgeCollectorPhysicalNodeVisitor;
import com.hazelcast.sql.impl.calcite.expression.ExpressionConverterRexVisitor;
import com.hazelcast.sql.impl.calcite.operators.HazelcastSqlOperatorTable;
import com.hazelcast.sql.impl.calcite.opt.AbstractScanRel;
import com.hazelcast.sql.impl.calcite.opt.ExplainCreator;
import com.hazelcast.sql.impl.calcite.opt.physical.FilterPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapIndexScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MaterializedInputPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ProjectPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ReplicatedMapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ReplicatedToDistributedPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.RootPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.SortPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.agg.AggregatePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.BroadcastExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.RootSingletonSortMergeExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.UnicastExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.join.HashJoinPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.join.NestedLoopJoinPhysicalRel;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.CountRowExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.aggregate.AggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.AverageAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.CountAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.DistributedAverageAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.MaxAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.MinAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.SingleValueAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.SumAggregateExpression;
import com.hazelcast.sql.impl.physical.AggregatePhysicalNode;
import com.hazelcast.sql.impl.physical.FilterPhysicalNode;
import com.hazelcast.sql.impl.physical.MapIndexScanPhysicalNode;
import com.hazelcast.sql.impl.physical.MapScanPhysicalNode;
import com.hazelcast.sql.impl.physical.MaterializedInputPhysicalNode;
import com.hazelcast.sql.impl.physical.PhysicalNode;
import com.hazelcast.sql.impl.physical.PhysicalNodeSchema;
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
import com.hazelcast.sql.impl.type.DataType;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

 /**
 * Visitor which produces executable query plan.
 */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:classfanoutcomplexity", "rawtypes"})
public class PlanCreateVisitor implements PhysicalRelVisitor {
    /** ID of query coordinator. */
    private final UUID localMemberId;

    /** Partition mapping. */
    private final Map<UUID, PartitionIdSet> partMap;

    /** Data member IDs. */
    private final List<UUID> dataMemberIds;

    /** Data members in a form of set. */
    private final Set<UUID> dataMemberIdsSet;

    /** Data member addresses. */
    private final List<Address> dataMemberAddresses;

    /** Rel ID map. */
    private final Map<PhysicalRel, List<Integer>> relIdMap;

    /** Original SQL. */
    private final String sql;

    /** Number of parameters. */
    private final int paramsCount;

    /** Optimizer statistics. */
    private final OptimizerStatistics stats;

    /** Prepared fragments. */
    private final List<QueryFragment> fragments = new ArrayList<>();

    /** Upstream nodes. Normally it is a one node, except of multi-source operations (e.g. joins, sets, subqueries). */
    private final Deque<PhysicalNode> upstreamNodes = new ArrayDeque<>();

    /** ID of current edge. */
    private int nextEdgeGenerator;

    /** Root physical rel. */
    private RootPhysicalRel rootPhysicalRel;

    public PlanCreateVisitor(
        UUID localMemberId,
        Map<UUID, PartitionIdSet> partMap,
        List<UUID> dataMemberIds,
        List<Address> dataMemberAddresses,
        Map<PhysicalRel, List<Integer>> relIdMap,
        String sql,
        int paramsCount,
        OptimizerStatistics stats
    ) {
        this.localMemberId = localMemberId;
        this.partMap = partMap;
        this.dataMemberIds = dataMemberIds;
        this.dataMemberAddresses = dataMemberAddresses;
        this.relIdMap = relIdMap;
        this.sql = sql;
        this.paramsCount = paramsCount;
        this.stats = stats;

        dataMemberIdsSet = new HashSet<>(dataMemberIds);
    }

    public QueryPlan getPlan() {
        // Calculate edges.
        Map<Integer, Integer> outboundEdgeMap = new HashMap<>();
        Map<Integer, Integer> inboundEdgeMap = new HashMap<>();
        Map<Integer, Integer> inboundEdgeMemberCountMap = new HashMap<>();

        for (int i = 0; i < fragments.size(); i++) {
            QueryFragment fragment = fragments.get(i);

            Integer outboundEdge = fragment.getOutboundEdge();

            if (outboundEdge != null) {
                outboundEdgeMap.put(outboundEdge, i);
            }

            if (fragment.getInboundEdges() != null) {
                for (Integer inboundEdge : fragment.getInboundEdges()) {
                    inboundEdgeMap.put(inboundEdge, i);
                    inboundEdgeMemberCountMap.put(inboundEdge, fragment.getMapping().getMemberCount());
                }
            }
        }

        // Calculate edge

        assert rootPhysicalRel != null;

        QueryExplain explain = ExplainCreator.explain(sql, rootPhysicalRel);

        return new QueryPlan(
            partMap,
            dataMemberIds,
            dataMemberAddresses,
            fragments,
            outboundEdgeMap,
            inboundEdgeMap,
            inboundEdgeMemberCountMap,
            paramsCount,
            explain,
            stats
        );
    }

    @Override
    public void onRoot(RootPhysicalRel rel) {
        rootPhysicalRel = rel;

        PhysicalNode upstreamNode = pollSingleUpstream();

        RootPhysicalNode rootNode = new RootPhysicalNode(
            pollId(rel),
            upstreamNode
        );

        addFragment(rootNode, QueryFragmentMapping.staticMapping(Collections.singleton(localMemberId)));
    }

    @Override
    public void onMapScan(MapScanPhysicalRel rel) {
        List<String> fieldPaths = getScanFieldPaths(rel);

        PhysicalNodeSchema schemaBefore = getScanSchemaBeforeProject(rel);

        MapScanPhysicalNode scanNode = new MapScanPhysicalNode(
            pollId(rel),
            rel.getTableUnwrapped().getName(),
            fieldPaths,
            schemaBefore.getTypes(),
            rel.getProjects(),
            convertFilter(schemaBefore, rel.getFilter())
        );

        pushUpstream(scanNode);
    }

    @Override
    public void onMapIndexScan(MapIndexScanPhysicalRel rel) {
        List<String> fieldPaths = getScanFieldPaths(rel);

        PhysicalNodeSchema schemaBefore = getScanSchemaBeforeProject(rel);

        MapIndexScanPhysicalNode scanNode = new MapIndexScanPhysicalNode(
            pollId(rel),
            rel.getTableUnwrapped().getName(),
            fieldPaths,
            schemaBefore.getTypes(),
            rel.getProjects(),
            rel.getIndex().getName(),
            rel.getIndexFilter(),
            convertFilter(schemaBefore, rel.getRemainderFilter())
        );

        pushUpstream(scanNode);
    }

    @Override
    public void onReplicatedMapScan(ReplicatedMapScanPhysicalRel rel) {
        List<String> fieldPaths = getScanFieldPaths(rel);

        PhysicalNodeSchema schemaBefore = getScanSchemaBeforeProject(rel);

        ReplicatedMapScanPhysicalNode scanNode = new ReplicatedMapScanPhysicalNode(
            pollId(rel),
            rel.getTableUnwrapped().getName(),
            fieldPaths,
            schemaBefore.getTypes(),
            rel.getProjects(),
            convertFilter(schemaBefore, rel.getFilter())
        );

        pushUpstream(scanNode);
    }

    @Override
    public void onSort(SortPhysicalRel rel) {
        PhysicalNode upstreamNode = pollSingleUpstream();
        PhysicalNodeSchema upstreamNodeSchema = upstreamNode.getSchema();

        List<RelFieldCollation> collations = rel.getCollation().getFieldCollations();

        List<Expression> expressions = new ArrayList<>(collations.size());
        List<Boolean> ascs = new ArrayList<>(collations.size());

        for (RelFieldCollation collation : collations) {
            RelFieldCollation.Direction direction = collation.getDirection();
            int idx = collation.getFieldIndex();

            expressions.add(ColumnExpression.create(idx, upstreamNodeSchema.getType(idx)));
            ascs.add(!direction.isDescending());
        }

        SortPhysicalNode sortNode = new SortPhysicalNode(
            pollId(rel),
            upstreamNode,
            expressions,
            ascs
        );

        pushUpstream(sortNode);
    }

    @Override
    public void onUnicastExchange(UnicastExchangePhysicalRel rel) {
        // Get upstream node.
        PhysicalNode upstreamNode = pollSingleUpstream();

        // Create sender and push it as a fragment.
        int edge = nextEdge();

        int id = pollId(rel);

        UnicastSendPhysicalNode sendNode = new UnicastSendPhysicalNode(
            id,
            upstreamNode,
            edge,
            new FieldHashFunction(rel.getHashFields())
        );

        addFragment(sendNode, QueryFragmentMapping.staticMapping(dataMemberIdsSet));

        // Create receiver.
        ReceivePhysicalNode receiveNode = new ReceivePhysicalNode(
            id,
            edge,
            sendNode.getSchema().getTypes()
        );

        pushUpstream(receiveNode);
    }

    @Override
    public void onBroadcastExchange(BroadcastExchangePhysicalRel rel) {
        // Get upstream node.
        PhysicalNode upstreamNode = pollSingleUpstream();

        // Create sender and push it as a fragment.
        int edge = nextEdge();

        int id = pollId(rel);

        BroadcastSendPhysicalNode sendNode = new BroadcastSendPhysicalNode(
            id,
            upstreamNode,
            edge
        );

        addFragment(sendNode, QueryFragmentMapping.staticMapping(dataMemberIdsSet));

        // Create receiver.
        ReceivePhysicalNode receiveNode = new ReceivePhysicalNode(
            id,
            edge,
            sendNode.getSchema().getTypes()
        );

        pushUpstream(receiveNode);
    }

    @Override
    public void onSingletonSortMergeExchange(RootSingletonSortMergeExchangePhysicalRel rel) {
        // Get upstream node. It should be sort node.
        PhysicalNode upstreamNode = pollSingleUpstream();

        assert upstreamNode instanceof SortPhysicalNode;

        SortPhysicalNode sortNode = (SortPhysicalNode) upstreamNode;

        // Create sender and push it as a fragment.
        int edge = nextEdge();

        int id = pollId(rel);

        UnicastSendPhysicalNode sendNode = new UnicastSendPhysicalNode(
            id,
            sortNode,
            edge,
            AllFieldsHashFunction.INSTANCE
        );

        addFragment(sendNode, QueryFragmentMapping.staticMapping(dataMemberIdsSet));

        // Create a receiver and push it to stack.
        ReceiveSortMergePhysicalNode receiveNode = new ReceiveSortMergePhysicalNode(
            id,
            edge,
            sendNode.getSchema().getTypes(),
            sortNode.getExpressions(),
            sortNode.getAscs()
        );

        pushUpstream(receiveNode);
    }

     @Override
     public void onReplicatedToDistributed(ReplicatedToDistributedPhysicalRel rel) {
         PhysicalNode upstreamNode = pollSingleUpstream();

         ReplicatedToPartitionedPhysicalNode replicatedToPartitionedNode = new ReplicatedToPartitionedPhysicalNode(
             pollId(rel),
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
            Expression convertedProject = convertExpression(upstreamNode.getSchema(), project);

            convertedProjects.add(convertedProject);
        }

        ProjectPhysicalNode projectNode = new ProjectPhysicalNode(
            pollId(rel),
            upstreamNode,
            convertedProjects
        );

        pushUpstream(projectNode);
    }

    @Override
    public void onFilter(FilterPhysicalRel rel) {
        PhysicalNode upstreamNode = pollSingleUpstream();

        Expression<Boolean> filter = convertFilter(upstreamNode.getSchema(), rel.getCondition());

        FilterPhysicalNode filterNode = new FilterPhysicalNode(
            pollId(rel),
            upstreamNode,
            filter
        );

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
            AggregateExpression aggAccumulator = convertAggregateCall(aggCall, upstreamNode.getSchema());

            aggAccumulators.add(aggAccumulator);
        }

        AggregatePhysicalNode aggNode = new AggregatePhysicalNode(
            pollId(rel),
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

         PhysicalNodeSchema schema = PhysicalNodeSchema.combine(leftInput.getSchema(), rightInput.getSchema());

         RexNode condition = rel.getCondition();
         Expression convertedCondition = convertExpression(schema, condition);

         NestedLoopJoinPhysicalNode joinNode = new NestedLoopJoinPhysicalNode(
             pollId(rel),
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

         PhysicalNodeSchema schema = PhysicalNodeSchema.combine(leftInput.getSchema(), rightInput.getSchema());

         RexNode condition = rel.getCondition();
         Expression convertedCondition = convertExpression(schema, condition);

         HashJoinPhysicalNode joinNode = new HashJoinPhysicalNode(
             pollId(rel),
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

         MaterializedInputPhysicalNode node = new MaterializedInputPhysicalNode(
             pollId(rel),
             input
         );

         pushUpstream(node);
     }

     public static AggregateExpression convertAggregateCall(AggregateCall aggCall, PhysicalNodeSchema upstreamSchema) {
        SqlAggFunction aggFunc = aggCall.getAggregation();
        List<Integer> argList = aggCall.getArgList();

        boolean distinct = aggCall.isDistinct();

        switch (aggFunc.getKind()) {
            case SUM:
                return SumAggregateExpression.create(aggregateColumn(argList, 0, upstreamSchema), distinct);

            case COUNT:
                Expression operand;

                if (argList.isEmpty()) {
                    operand = CountRowExpression.INSTANCE;
                } else {
                    assert argList.size() == 1;

                    operand = aggregateColumn(argList, 0, upstreamSchema);
                }

                return CountAggregateExpression.create(operand, distinct);

            case MIN:
                return MinAggregateExpression.create(aggregateColumn(argList, 0, upstreamSchema), distinct);

            case MAX:
                return MaxAggregateExpression.create(aggregateColumn(argList, 0, upstreamSchema), distinct);

            case AVG:
                return AverageAggregateExpression.create(aggregateColumn(argList, 0, upstreamSchema), distinct);

            case SINGLE_VALUE:
                assert argList.size() == 1;

                // TODO: Make sure to eliminate the whole aggregate whose goal is to deduplicate rows if we can prove
                //  that the input is unique (e.g. __key is present, or unique index is used, etc.)
                return SingleValueAggregateExpression.create(aggregateColumn(argList, 0, upstreamSchema), distinct);

            case OTHER_FUNCTION:
                assert aggFunc == HazelcastSqlOperatorTable.DISTRIBUTED_AVG;

                return DistributedAverageAggregateExpression.create(
                    aggregateColumn(argList, 0, upstreamSchema),
                    aggregateColumn(argList, 1, upstreamSchema)
                );

            default:
                throw HazelcastSqlException.error("Unsupported aggregate call: " + aggFunc.getName());
        }
    }

     private static ColumnExpression<?> aggregateColumn(List<Integer> argList, int index, PhysicalNodeSchema upstreamSchema) {
         Integer columnIndex = argList.get(index);
         DataType columnType = upstreamSchema.getType(columnIndex);

         return ColumnExpression.create(columnIndex, columnType);
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

    private int pollId(PhysicalRel rel) {
        List<Integer> ids = relIdMap.get(rel);

        assert ids != null;
        assert !ids.isEmpty();

        return ids.remove(0);
    }

    @SuppressWarnings("unchecked")
    private Expression<Boolean> convertFilter(PhysicalNodeSchema schema, RexNode expression) {
        if (expression == null) {
            return null;
        }

        Expression convertedExpression = convertExpression(schema, expression);

        return (Expression<Boolean>) convertedExpression;
    }

    private Expression convertExpression(PhysicalNodeSchema schema, RexNode expression) {
        ExpressionConverterRexVisitor converter = new ExpressionConverterRexVisitor(schema, paramsCount);

        return expression.accept(converter);
    }

    private static PhysicalNodeSchema getScanSchemaBeforeProject(AbstractScanRel rel) {
        HazelcastTable table = rel.getTableUnwrapped();

        List<DataType> types = new ArrayList<>();

        for (String fieldName : rel.getTable().getRowType().getFieldNames()) {
            types.add(table.getFieldType(fieldName));
        }

        return new PhysicalNodeSchema(types);
    }

    private static List<String> getScanFieldPaths(AbstractScanRel rel) {
        HazelcastTable table = rel.getTableUnwrapped();

        List<String> paths = new ArrayList<>();

        for (String fieldName : rel.getTable().getRowType().getFieldNames()) {
            paths.add(table.getFieldPath(fieldName));
        }

        return paths;
     }
}
