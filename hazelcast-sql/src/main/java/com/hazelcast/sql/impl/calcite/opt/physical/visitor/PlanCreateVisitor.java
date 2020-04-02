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

import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryMetadata;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.calcite.EdgeCollectorPlanNodeVisitor;
import com.hazelcast.sql.impl.calcite.expression.RexToExpressionVisitor;
import com.hazelcast.sql.impl.calcite.operators.HazelcastSqlOperatorTable;
import com.hazelcast.sql.impl.calcite.opt.AbstractScanRel;
import com.hazelcast.sql.impl.calcite.opt.ExplainCreator;
import com.hazelcast.sql.impl.calcite.opt.physical.FetchPhysicalRel;
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
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.SortMergeExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.UnicastExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.join.HashJoinPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.join.NestedLoopJoinPhysicalRel;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.explain.QueryExplain;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.aggregate.CountRowExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.aggregate.AggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.AverageAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.CountAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.DistributedAverageAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.MaxAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.MinAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.SingleValueAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.SumAggregateExpression;
import com.hazelcast.sql.impl.plan.PlanFragment;
import com.hazelcast.sql.impl.plan.PlanFragmentMapping;
import com.hazelcast.sql.impl.optimizer.OptimizerStatistics;
import com.hazelcast.sql.impl.plan.node.AggregatePlanNode;
import com.hazelcast.sql.impl.plan.node.FetchOffsetPlanNodeFieldTypeProvider;
import com.hazelcast.sql.impl.plan.node.FetchPlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider;
import com.hazelcast.sql.impl.plan.node.FilterPlanNode;
import com.hazelcast.sql.impl.plan.node.MapIndexScanPlanNode;
import com.hazelcast.sql.impl.plan.node.MapScanPlanNode;
import com.hazelcast.sql.impl.plan.node.MaterializedInputPlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.plan.node.ProjectPlanNode;
import com.hazelcast.sql.impl.plan.node.ReplicatedMapScanPlanNode;
import com.hazelcast.sql.impl.plan.node.ReplicatedToPartitionedPlanNode;
import com.hazelcast.sql.impl.plan.node.RootPlanNode;
import com.hazelcast.sql.impl.plan.node.SortPlanNode;
import com.hazelcast.sql.impl.row.partitioner.AllFieldsRowPartitioner;
import com.hazelcast.sql.impl.row.partitioner.FieldsRowPartitioner;
import com.hazelcast.sql.impl.plan.node.io.BroadcastSendPlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceivePlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceiveSortMergePlanNode;
import com.hazelcast.sql.impl.plan.node.io.UnicastSendPlanNode;
import com.hazelcast.sql.impl.plan.node.join.HashJoinPlanNode;
import com.hazelcast.sql.impl.plan.node.join.NestedLoopJoinPlanNode;
import com.hazelcast.sql.impl.type.QueryDataType;
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

    /** Rel ID map. */
    private final Map<PhysicalRel, List<Integer>> relIdMap;

    /** Original SQL. */
    private final String sql;

    /** Number of parameters. */
    private final int paramsCount;

    /** Optimizer statistics. */
    private final OptimizerStatistics stats;

    /** Prepared fragments. */
    private final List<PlanFragment> fragments = new ArrayList<>();

    /** Upstream nodes. Normally it is a one node, except of multi-source operations (e.g. joins, sets, subqueries). */
    private final Deque<PlanNode> upstreamNodes = new ArrayDeque<>();

    /** ID of current edge. */
    private int nextEdgeGenerator;

    /** Root physical rel. */
    private RootPhysicalRel rootPhysicalRel;

    /** Root metadata. */
    private QueryMetadata rootMetadata;

    public PlanCreateVisitor(
        UUID localMemberId,
        Map<UUID, PartitionIdSet> partMap,
        List<UUID> dataMemberIds,
        Map<PhysicalRel, List<Integer>> relIdMap,
        String sql,
        int paramsCount,
        OptimizerStatistics stats
    ) {
        this.localMemberId = localMemberId;
        this.partMap = partMap;
        this.dataMemberIds = dataMemberIds;
        this.relIdMap = relIdMap;
        this.sql = sql;
        this.paramsCount = paramsCount;
        this.stats = stats;

        dataMemberIdsSet = new HashSet<>(dataMemberIds);
    }

    public Plan getPlan() {
        // Calculate edges.
        Map<Integer, Integer> outboundEdgeMap = new HashMap<>();
        Map<Integer, Integer> inboundEdgeMap = new HashMap<>();
        Map<Integer, Integer> inboundEdgeMemberCountMap = new HashMap<>();

        for (int i = 0; i < fragments.size(); i++) {
            PlanFragment fragment = fragments.get(i);

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

        assert rootPhysicalRel != null;
        assert rootMetadata != null;

        QueryExplain explain = ExplainCreator.explain(sql, rootPhysicalRel);

        return new Plan(
            partMap,
            dataMemberIds,
            fragments,
            outboundEdgeMap,
            inboundEdgeMap,
            inboundEdgeMemberCountMap,
            paramsCount,
            rootMetadata,
            explain,
            stats
        );
    }

    @Override
    public void onRoot(RootPhysicalRel rel) {
        rootPhysicalRel = rel;

        PlanNode upstreamNode = pollSingleUpstream();

        RootPlanNode rootNode = new RootPlanNode(
            pollId(rel),
            upstreamNode
        );

        rootMetadata = new QueryMetadata(rootNode.getSchema().getTypes());

        addFragment(rootNode, new PlanFragmentMapping(Collections.singleton(localMemberId), false));
    }

    @Override
    public void onMapScan(MapScanPhysicalRel rel) {
        List<String> fieldPaths = getScanFieldPaths(rel);

        PlanNodeSchema schemaBefore = getScanSchemaBeforeProject(rel);

        MapScanPlanNode scanNode = new MapScanPlanNode(
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

        PlanNodeSchema schemaBefore = getScanSchemaBeforeProject(rel);

        MapIndexScanPlanNode scanNode = new MapIndexScanPlanNode(
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

        PlanNodeSchema schemaBefore = getScanSchemaBeforeProject(rel);

        ReplicatedMapScanPlanNode scanNode = new ReplicatedMapScanPlanNode(
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
        PlanNode upstreamNode = pollSingleUpstream();
        PlanNodeSchema upstreamNodeSchema = upstreamNode.getSchema();

        List<RelFieldCollation> collations = rel.getCollation().getFieldCollations();

        List<Expression> expressions = new ArrayList<>(collations.size());
        List<Boolean> ascs = new ArrayList<>(collations.size());

        for (RelFieldCollation collation : collations) {
            RelFieldCollation.Direction direction = collation.getDirection();
            int idx = collation.getFieldIndex();

            expressions.add(ColumnExpression.create(idx, upstreamNodeSchema.getType(idx)));
            ascs.add(!direction.isDescending());
        }

        Expression fetch = convertExpression(FetchOffsetPlanNodeFieldTypeProvider.INSTANCE, rel.fetch);
        Expression offset = convertExpression(FetchOffsetPlanNodeFieldTypeProvider.INSTANCE, rel.offset);

        SortPlanNode sortNode = new SortPlanNode(
            pollId(rel),
            upstreamNode,
            expressions,
            ascs,
            fetch,
            offset
        );

        pushUpstream(sortNode);
    }

    @Override
    public void onUnicastExchange(UnicastExchangePhysicalRel rel) {
        // Get upstream node.
        PlanNode upstreamNode = pollSingleUpstream();

        // Create sender and push it as a fragment.
        int edge = nextEdge();

        int id = pollId(rel);

        UnicastSendPlanNode sendNode = new UnicastSendPlanNode(
            id,
            upstreamNode,
            edge,
            new FieldsRowPartitioner(rel.getHashFields())
        );

        addFragment(sendNode, dataMemberMapping());

        // Create receiver.
        ReceivePlanNode receiveNode = new ReceivePlanNode(
            id,
            edge,
            sendNode.getSchema().getTypes()
        );

        pushUpstream(receiveNode);
    }

    @Override
    public void onBroadcastExchange(BroadcastExchangePhysicalRel rel) {
        // Get upstream node.
        PlanNode upstreamNode = pollSingleUpstream();

        // Create sender and push it as a fragment.
        int edge = nextEdge();

        int id = pollId(rel);

        BroadcastSendPlanNode sendNode = new BroadcastSendPlanNode(
            id,
            upstreamNode,
            edge
        );

        addFragment(sendNode, dataMemberMapping());

        // Create receiver.
        ReceivePlanNode receiveNode = new ReceivePlanNode(
            id,
            edge,
            sendNode.getSchema().getTypes()
        );

        pushUpstream(receiveNode);
    }

    @Override
    public void onSortMergeExchange(SortMergeExchangePhysicalRel rel) {
        // Get upstream node. It should be sort node.
        PlanNode upstreamNode = pollSingleUpstream();

        assert upstreamNode instanceof SortPlanNode;

        SortPlanNode sortNode = (SortPlanNode) upstreamNode;

        // Create sender and push it as a fragment.
        int edge = nextEdge();

        int id = pollId(rel);

        UnicastSendPlanNode sendNode = new UnicastSendPlanNode(
            id,
            sortNode,
            edge,
            AllFieldsRowPartitioner.INSTANCE
        );

        addFragment(sendNode, dataMemberMapping());

        // Create a receiver and push it to stack.
        ReceiveSortMergePlanNode receiveNode = new ReceiveSortMergePlanNode(
            id,
            edge,
            sendNode.getSchema().getTypes(),
            sortNode.getExpressions(),
            sortNode.getAscs(),
            sortNode.getFetch(),
            sortNode.getOffset()
        );

        pushUpstream(receiveNode);
    }

     @Override
     public void onReplicatedToDistributed(ReplicatedToDistributedPhysicalRel rel) {
         PlanNode upstreamNode = pollSingleUpstream();

         ReplicatedToPartitionedPlanNode replicatedToPartitionedNode = new ReplicatedToPartitionedPlanNode(
             pollId(rel),
             upstreamNode,
             new FieldsRowPartitioner(rel.getHashFields())
         );

         pushUpstream(replicatedToPartitionedNode);
     }

     @Override
    public void onProject(ProjectPhysicalRel rel) {
        PlanNode upstreamNode = pollSingleUpstream();

        List<RexNode> projects = rel.getProjects();
        List<Expression> convertedProjects = new ArrayList<>(projects.size());

        for (RexNode project : projects) {
            Expression convertedProject = convertExpression(upstreamNode.getSchema(), project);

            convertedProjects.add(convertedProject);
        }

        ProjectPlanNode projectNode = new ProjectPlanNode(
            pollId(rel),
            upstreamNode,
            convertedProjects
        );

        pushUpstream(projectNode);
    }

    @Override
    public void onFilter(FilterPhysicalRel rel) {
        PlanNode upstreamNode = pollSingleUpstream();

        Expression<Boolean> filter = convertFilter(upstreamNode.getSchema(), rel.getCondition());

        FilterPlanNode filterNode = new FilterPlanNode(
            pollId(rel),
            upstreamNode,
            filter
        );

        pushUpstream(filterNode);
    }

    @Override
    public void onAggregate(AggregatePhysicalRel rel) {
        PlanNode upstreamNode = pollSingleUpstream();

        List<Integer> groupKey = rel.getGroupSet().toList();
        int sortedGroupKeySize = rel.getSortedGroupSet().toList().size();

        List<AggregateCall> aggCalls = rel.getAggCallList();

        List<AggregateExpression> aggAccumulators = new ArrayList<>();

        for (AggregateCall aggCall : aggCalls) {
            AggregateExpression aggAccumulator = convertAggregateCall(aggCall, upstreamNode.getSchema());

            aggAccumulators.add(aggAccumulator);
        }

        AggregatePlanNode aggNode = new AggregatePlanNode(
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
         PlanNode leftInput = pollSingleUpstream();
         PlanNode rightInput = pollSingleUpstream();

         PlanNodeSchema schema = PlanNodeSchema.combine(leftInput.getSchema(), rightInput.getSchema());

         RexNode condition = rel.getCondition();
         Expression convertedCondition = convertExpression(schema, condition);

         NestedLoopJoinPlanNode joinNode = new NestedLoopJoinPlanNode(
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
         PlanNode leftInput = pollSingleUpstream();
         PlanNode rightInput = pollSingleUpstream();

         PlanNodeSchema schema = PlanNodeSchema.combine(leftInput.getSchema(), rightInput.getSchema());

         RexNode condition = rel.getCondition();
         Expression convertedCondition = convertExpression(schema, condition);

         HashJoinPlanNode joinNode = new HashJoinPlanNode(
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
         PlanNode input = pollSingleUpstream();

         MaterializedInputPlanNode node = new MaterializedInputPlanNode(
             pollId(rel),
             input
         );

         pushUpstream(node);
     }

     @Override
     public void onFetch(FetchPhysicalRel rel) {
         PlanNode input = pollSingleUpstream();

         Expression fetch = convertExpression(FetchOffsetPlanNodeFieldTypeProvider.INSTANCE, rel.getFetch());
         Expression offset = convertExpression(FetchOffsetPlanNodeFieldTypeProvider.INSTANCE, rel.getOffset());

         FetchPlanNode node = new FetchPlanNode(
             pollId(rel),
             input,
             fetch,
             offset
         );

         pushUpstream(node);
     }

     public static AggregateExpression convertAggregateCall(AggregateCall aggCall, PlanNodeSchema upstreamSchema) {
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

     private static ColumnExpression<?> aggregateColumn(List<Integer> argList, int index, PlanNodeSchema upstreamSchema) {
         Integer columnIndex = argList.get(index);
         QueryDataType columnType = upstreamSchema.getType(columnIndex);

         return ColumnExpression.create(columnIndex, columnType);
     }

    /**
     * Push node to upstream stack.
     *
     * @param node Node.
     */
    private void pushUpstream(PlanNode node) {
        upstreamNodes.addFirst(node);
    }

    /**
     * Poll an upstream node which is expected to be the only available in the stack.
     *
     * @return Upstream node.
     */
    private PlanNode pollSingleUpstream() {
        return upstreamNodes.pollFirst();
    }

    /**
     * Create new fragment and clear intermediate state.
     *
     * @param node Node.
     * @param mapping Fragment mapping mode.
     */
    private void addFragment(PlanNode node, PlanFragmentMapping mapping) {
        EdgeCollectorPlanNodeVisitor edgeVisitor = new EdgeCollectorPlanNodeVisitor();

        node.visit(edgeVisitor);

        Integer outboundEdge = edgeVisitor.getOutboundEdge();
        List<Integer> inboundEdges = edgeVisitor.getInboundEdges();

        PlanFragment fragment = new PlanFragment(
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
    private Expression<Boolean> convertFilter(PlanNodeSchema schema, RexNode expression) {
        if (expression == null) {
            return null;
        }

        Expression convertedExpression = convertExpression(schema, expression);

        return (Expression<Boolean>) convertedExpression;
    }

    private Expression convertExpression(PlanNodeFieldTypeProvider fieldTypeProvider, RexNode expression) {
        if (expression == null) {
            return null;
        }

        RexToExpressionVisitor converter = new RexToExpressionVisitor(fieldTypeProvider, paramsCount);

        return expression.accept(converter);
    }

    private static PlanNodeSchema getScanSchemaBeforeProject(AbstractScanRel rel) {
        HazelcastTable table = rel.getTableUnwrapped();

        List<QueryDataType> types = new ArrayList<>();

        for (String fieldName : rel.getTable().getRowType().getFieldNames()) {
            types.add(table.getFieldType(fieldName));
        }

        return new PlanNodeSchema(types);
    }

    private static List<String> getScanFieldPaths(AbstractScanRel rel) {
        HazelcastTable table = rel.getTableUnwrapped();

        List<String> paths = new ArrayList<>();

        for (String fieldName : rel.getTable().getRowType().getFieldNames()) {
            paths.add(table.getFieldPath(fieldName));
        }

        return paths;
    }

    // TODO: Data member mapping should be used only for fragments with scans!
    private PlanFragmentMapping dataMemberMapping() {
        return new PlanFragmentMapping(dataMemberIdsSet, true);
    }
}
