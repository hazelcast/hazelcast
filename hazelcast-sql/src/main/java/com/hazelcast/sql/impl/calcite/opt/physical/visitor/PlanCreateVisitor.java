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
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.QueryUtils;
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
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.AbstractExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.BroadcastExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.RootExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.SortMergeExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.UnicastExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.join.HashJoinPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.join.NestedLoopJoinPhysicalRel;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable;
import com.hazelcast.sql.impl.explain.QueryExplain;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.aggregate.AggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.AverageAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.CountAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.CountRowExpression;
import com.hazelcast.sql.impl.expression.aggregate.DistributedAverageAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.MaxAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.MinAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.SingleValueAggregateExpression;
import com.hazelcast.sql.impl.expression.aggregate.SumAggregateExpression;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.partitioner.AllFieldsRowPartitioner;
import com.hazelcast.sql.impl.partitioner.FieldsRowPartitioner;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.plan.PlanFragmentMapping;
import com.hazelcast.sql.impl.plan.cache.PlanCacheKey;
import com.hazelcast.sql.impl.plan.cache.PlanObjectId;
import com.hazelcast.sql.impl.plan.node.AggregatePlanNode;
import com.hazelcast.sql.impl.plan.node.FetchOffsetPlanNodeFieldTypeProvider;
import com.hazelcast.sql.impl.plan.node.FetchPlanNode;
import com.hazelcast.sql.impl.plan.node.FilterPlanNode;
import com.hazelcast.sql.impl.plan.node.MapIndexScanPlanNode;
import com.hazelcast.sql.impl.plan.node.MapScanPlanNode;
import com.hazelcast.sql.impl.plan.node.MaterializedInputPlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.plan.node.ProjectPlanNode;
import com.hazelcast.sql.impl.plan.node.ReplicatedMapScanPlanNode;
import com.hazelcast.sql.impl.plan.node.ReplicatedToPartitionedPlanNode;
import com.hazelcast.sql.impl.plan.node.RootPlanNode;
import com.hazelcast.sql.impl.plan.node.SortPlanNode;
import com.hazelcast.sql.impl.plan.node.io.BroadcastSendPlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceivePlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceiveSortMergePlanNode;
import com.hazelcast.sql.impl.plan.node.io.RootSendPlanNode;
import com.hazelcast.sql.impl.plan.node.io.UnicastSendPlanNode;
import com.hazelcast.sql.impl.plan.node.join.HashJoinPlanNode;
import com.hazelcast.sql.impl.plan.node.join.NestedLoopJoinPlanNode;
import com.hazelcast.sql.impl.schema.map.AbstractMapTable;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;

import javax.annotation.Nullable;
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
 * Visitor which produces executable query plan from a relational operator tree.
 * <p>
 * Plan is a sequence of fragments connected via send/receive operators. A fragment is a sequence of relational operators.
 * <p>
 * Most relational operators have one-to-one mapping to the appropriate {@link PlanNode}.
 * The exception is the family of {@link AbstractExchangePhysicalRel} operators. When an exchange is met, a new fragment is
 * created, and then exchange is converted into a pair of appropriate send/receive operators. Send operator is added to the
 * previous fragment, receive operator is a starting point for the new fragment.
 */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:classfanoutcomplexity", "checkstyle:MethodCount",
    "rawtypes"})
public class PlanCreateVisitor implements PhysicalRelVisitor {
    /** ID of query coordinator. */
    private final UUID localMemberId;

    /** Partition mapping. */
    private final Map<UUID, PartitionIdSet> partMap;

    /** Participant IDs. */
    private final Set<UUID> memberIds;

    /** Rel ID map. */
    private final Map<PhysicalRel, List<Integer>> relIdMap;

    /** Original SQL. */
    private final String sql;

    private final PlanCacheKey planKey;

    private final QueryParameterMetadata parameterMetadata;

    /** Names of the returned columns from the original query. */
    private final List<String> rootColumnNames;

    /** Prepared fragments. */
    private final List<PlanNode> fragments = new ArrayList<>();

    /** Fragment mappings. */
    private final List<PlanFragmentMapping> fragmentMappings = new ArrayList<>();

    /** Outbound edge of the fragment. */
    private final List<Integer> fragmentOutboundEdge = new ArrayList<>();

    /** Inbound edges of the fragment. */
    private final List<List<Integer>> fragmentInboundEdges = new ArrayList<>();

    /** Upstream nodes. Normally it is one node, except for multi-source operations (e.g. joins, sets, subqueries). */
    private final Deque<PlanNode> upstreamNodes = new ArrayDeque<>();

    /** ID of current edge. */
    private int nextEdgeGenerator;

    /** Root physical rel. */
    private RootPhysicalRel rootPhysicalRel;

    /** Row metadata. */
    private SqlRowMetadata rowMetadata;

    /** Collected IDs of objects used in the plan. */
    private final Set<PlanObjectId> objectIds = new HashSet<>();

    /**
     * @param rootColumnNames Root column names. They are null when called from
     *     Jet for a sub-relNode and the row metadata aren't needed
     */
    public PlanCreateVisitor(
        UUID localMemberId,
        Map<UUID, PartitionIdSet> partMap,
        Map<PhysicalRel, List<Integer>> relIdMap,
        String sql,
        PlanCacheKey planKey,
        QueryParameterMetadata parameterMetadata,
        @Nullable List<String> rootColumnNames
    ) {
        this.localMemberId = localMemberId;
        this.partMap = partMap;
        this.relIdMap = relIdMap;
        this.sql = sql;
        this.planKey = planKey;
        this.parameterMetadata = parameterMetadata;
        this.rootColumnNames = rootColumnNames;

        memberIds = new HashSet<>(partMap.keySet());
    }

    public Plan getPlan() {
        // Calculate edges.
        Map<Integer, Integer> outboundEdgeMap = new HashMap<>();
        Map<Integer, Integer> inboundEdgeMap = new HashMap<>();
        Map<Integer, Integer> inboundEdgeMemberCountMap = new HashMap<>();

        for (int i = 0; i < fragments.size(); i++) {
            PlanFragmentMapping fragmentMapping = fragmentMappings.get(i);
            Integer outboundEdge = fragmentOutboundEdge.get(i);
            List<Integer> inboundEdges = fragmentInboundEdges.get(i);

            if (outboundEdge != null) {
                outboundEdgeMap.put(outboundEdge, i);
            }

            if (inboundEdges != null) {
                for (Integer inboundEdge : inboundEdges) {
                    int count = fragmentMapping.isDataMembers() ? partMap.size() : fragmentMapping.getMemberIds().size();

                    inboundEdgeMap.put(inboundEdge, i);
                    inboundEdgeMemberCountMap.put(inboundEdge, count);
                }
            }
        }

        assert rootPhysicalRel != null;
        assert rowMetadata != null ^ rootColumnNames == null;

        QueryExplain explain = ExplainCreator.explain(sql, rootPhysicalRel);

        return new Plan(
            partMap,
            fragments,
            fragmentMappings,
            outboundEdgeMap,
            inboundEdgeMap,
            inboundEdgeMemberCountMap,
            parameterMetadata,
            rowMetadata,
            planKey,
            explain,
            objectIds
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

        if (rootColumnNames != null) {
            rowMetadata = createRowMetadata(rootColumnNames, rootNode.getSchema().getTypes());
        }

        addFragment(rootNode, new PlanFragmentMapping(Collections.singleton(localMemberId), false));
    }

    private static SqlRowMetadata createRowMetadata(List<String> columnNames, List<QueryDataType> columnTypes) {
        assert columnNames.size() == columnTypes.size();

        List<SqlColumnMetadata> columns = new ArrayList<>(columnNames.size());

        for (int i = 0; i < columnNames.size(); i++) {
            SqlColumnMetadata column = QueryUtils.getColumnMetadata(columnNames.get(i), columnTypes.get(i));

            columns.add(column);
        }

        return new SqlRowMetadata(columns);
    }

    @Override
    public void onMapScan(MapScanPhysicalRel rel) {
        AbstractMapTable table = rel.getMap();

        if (((PartitionedMapTable) table).isHd()) {
            throw QueryException.error("Cannot query IMap " + table.getName()
                    + " with InMemoryFormat.NATIVE because it does not have indexes "
                    + "(please add at least one index to query this IMap)");
        }

        HazelcastTable hazelcastTable = rel.getTableUnwrapped();

        PlanNodeSchema schemaBefore = getScanSchemaBeforeProject(table);

        MapScanPlanNode scanNode = new MapScanPlanNode(
            pollId(rel),
            table.getName(),
            table.getKeyDescriptor(),
            table.getValueDescriptor(),
            getScanFieldPaths(table),
            schemaBefore.getTypes(),
            hazelcastTable.getProjects(),
            convertFilter(schemaBefore, hazelcastTable.getFilter())
        );

        pushUpstream(scanNode);

        objectIds.add(table.getObjectId());
    }

    @Override
    public void onMapIndexScan(MapIndexScanPhysicalRel rel) {
        HazelcastTable hazelcastTable = rel.getTableUnwrapped();
        AbstractMapTable table = rel.getMap();

        PlanNodeSchema schemaBefore = getScanSchemaBeforeProject(table);

        MapIndexScanPlanNode scanNode = new MapIndexScanPlanNode(
            pollId(rel),
            table.getName(),
            table.getKeyDescriptor(),
            table.getValueDescriptor(),
            getScanFieldPaths(table),
            schemaBefore.getTypes(),
            hazelcastTable.getProjects(),
            rel.getIndex().getName(),
            rel.getIndex().getComponentsCount(),
            rel.getIndexFilter(),
            rel.getConverterTypes(),
            convertFilter(schemaBefore, rel.getRemainderExp())
        );

        pushUpstream(scanNode);

        objectIds.add(table.getObjectId());
    }

    @Override
    public void onReplicatedMapScan(ReplicatedMapScanPhysicalRel rel) {
        AbstractMapTable table = rel.getMap();

        HazelcastTable hazelcastTable = rel.getTableUnwrapped();

        PlanNodeSchema schemaBefore = getScanSchemaBeforeProject(table);

        ReplicatedMapScanPlanNode scanNode = new ReplicatedMapScanPlanNode(
            pollId(rel),
            table.getName(),
            table.getKeyDescriptor(),
            table.getValueDescriptor(),
            getScanFieldPaths(table),
            schemaBefore.getTypes(),
            hazelcastTable.getProjects(),
            convertFilter(schemaBefore, hazelcastTable.getFilter())
        );

        pushUpstream(scanNode);

        objectIds.add(table.getObjectId());
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
    public void onRootExchange(RootExchangePhysicalRel rel) {
        // Get upstream node.
        PlanNode upstreamNode = pollSingleUpstream();

        // Create sender and push it as a fragment.
        int edge = nextEdge();

        int id = pollId(rel);

        RootSendPlanNode sendNode = new RootSendPlanNode(
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
                throw QueryException.error("Unsupported aggregate call: " + aggFunc.getName());
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

        fragments.add(node);
        fragmentMappings.add(mapping);
        fragmentOutboundEdge.add(edgeVisitor.getOutboundEdge());
        fragmentInboundEdges.add(edgeVisitor.getInboundEdges());
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

        RexToExpressionVisitor converter = new RexToExpressionVisitor(fieldTypeProvider, parameterMetadata);

        return expression.accept(converter);
    }

    private static PlanNodeSchema getScanSchemaBeforeProject(AbstractMapTable table) {
        List<QueryDataType> types = new ArrayList<>(table.getFieldCount());

        for (int i = 0; i < table.getFieldCount(); i++) {
            MapTableField field = table.getField(i);

            types.add(field.getType());
        }

        return new PlanNodeSchema(types);
    }

    private static List<QueryPath> getScanFieldPaths(AbstractMapTable table) {
        List<QueryPath> res = new ArrayList<>(table.getFieldCount());

        for (int i = 0; i < table.getFieldCount(); i++) {
            MapTableField field = table.getField(i);

            res.add(field.getPath());
        }

        return res;
    }

    // TODO: Data member mapping should be used only for fragments with scans!
    private PlanFragmentMapping dataMemberMapping() {
        return new PlanFragmentMapping(memberIds, true);
    }
}
