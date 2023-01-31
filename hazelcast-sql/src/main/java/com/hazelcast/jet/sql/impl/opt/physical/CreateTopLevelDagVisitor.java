/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.cluster.Address;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.MutableByte;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.function.KeyedWindowResultFunction;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.HazelcastPhysicalScan;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.ObjectArrayKey;
import com.hazelcast.jet.sql.impl.aggregate.WindowUtils;
import com.hazelcast.jet.sql.impl.connector.CalciteNode;
import com.hazelcast.jet.sql.impl.connector.SqlConnector.VertexWithInputConfig;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.jet.sql.impl.opt.ExpressionValues;
import com.hazelcast.jet.sql.impl.opt.WatermarkKeysAssigner;
import com.hazelcast.jet.sql.impl.opt.WatermarkThrottlingFrameSizeCalculator;
import com.hazelcast.jet.sql.impl.processors.LateItemsDropP;
import com.hazelcast.jet.sql.impl.processors.SqlHashJoinP;
import com.hazelcast.jet.sql.impl.processors.StreamToStreamJoinP.StreamToStreamJoinProcessorSupplier;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexProgram;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.function.Functions.entryKey;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.core.Vertex.LOCAL_PARALLELISM_USE_DEFAULT;
import static com.hazelcast.jet.core.processor.Processors.flatMapUsingServiceP;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.Processors.mapUsingServiceP;
import static com.hazelcast.jet.core.processor.Processors.sortP;
import static com.hazelcast.jet.core.processor.SourceProcessors.convenientSourceP;
import static com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil.getJetSqlConnector;
import static com.hazelcast.jet.sql.impl.processors.RootResultConsumerSink.rootResultConsumerSink;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

public class CreateTopLevelDagVisitor extends CreateDagVisitorBase<Vertex> {

    // TODO https://github.com/hazelcast/hazelcast/issues/20383
    public static final ExpressionEvalContext MOCK_EEC =
            new ExpressionEvalContext(emptyList(), new DefaultSerializationServiceBuilder().build());

    private static final int LOW_PRIORITY = 10;
    private static final int HIGH_PRIORITY = 1;

    private final Set<PlanObjectKey> objectKeys = new HashSet<>();
    private final NodeEngine nodeEngine;
    private final Address localMemberAddress;
    private final WatermarkKeysAssigner watermarkKeysAssigner;
    private long watermarkThrottlingFrameSize = -1;

    private final DagBuildContextImpl dagBuildContext;

    public CreateTopLevelDagVisitor(
            NodeEngine nodeEngine,
            QueryParameterMetadata parameterMetadata,
            @Nullable WatermarkKeysAssigner watermarkKeysAssigner,
            Set<PlanObjectKey> usedViews
    ) {
        super(new DAG());
        this.nodeEngine = nodeEngine;
        this.localMemberAddress = nodeEngine.getThisAddress();
        this.watermarkKeysAssigner = watermarkKeysAssigner;
        this.objectKeys.addAll(usedViews);

        dagBuildContext = new DagBuildContextImpl(getDag(), parameterMetadata);
    }

    @Override
    public Vertex onValues(ValuesPhysicalRel rel) {
        List<ExpressionValues> values = rel.values();

        return dag.newUniqueVertex("Values", convenientSourceP(
                ExpressionEvalContext::from,
                (context, buffer) -> {
                    values.forEach(vs -> vs.toValues(context).forEach(buffer::add));
                    buffer.close();
                },
                ctx -> null,
                (ctx, states) -> {
                },
                ConsumerEx.noop(),
                0,
                true,
                null)
        );
    }

    @Override
    public Vertex onInsert(InsertPhysicalRel rel) {
        watermarkThrottlingFrameSize = WatermarkThrottlingFrameSizeCalculator.calculate((PhysicalRel) rel.getInput());

        Table table = rel.getTable().unwrap(HazelcastTable.class).getTarget();
        collectObjectKeys(table);

        dagBuildContext.setTable(table);
        dagBuildContext.setRel(rel);
        VertexWithInputConfig vertexWithConfig = getJetSqlConnector(table).insertProcessor(dagBuildContext);
        Vertex vertex = vertexWithConfig.vertex();
        connectInput(rel.getInput(), vertex, vertexWithConfig.configureEdgeFn());
        return vertex;
    }

    @Override
    public Vertex onSink(SinkPhysicalRel rel) {
        watermarkThrottlingFrameSize = WatermarkThrottlingFrameSizeCalculator.calculate((PhysicalRel) rel.getInput());

        Table table = rel.getTable().unwrap(HazelcastTable.class).getTarget();
        collectObjectKeys(table);

        dagBuildContext.setTable(table);
        dagBuildContext.setRel(rel);
        Vertex vertex = getJetSqlConnector(table).sinkProcessor(dagBuildContext);
        connectInput(rel.getInput(), vertex, null);
        return vertex;
    }

    @Override
    public Vertex onUpdate(UpdatePhysicalRel rel) {
        // currently it's not possible to have a unbounded UPDATE, but if we do, we'd need this calculation
        watermarkThrottlingFrameSize = WatermarkThrottlingFrameSizeCalculator.calculate((PhysicalRel) rel.getInput());

        Table table = rel.getTable().unwrap(HazelcastTable.class).getTarget();

        dagBuildContext.setTable(table);
        dagBuildContext.setRel(rel);
        Vertex vertex = getJetSqlConnector(table).updateProcessor(
                dagBuildContext, rel.getUpdateColumnList(), CalciteNode.projection(rel.getSourceExpressionList()));
        connectInput(rel.getInput(), vertex, null);
        return vertex;
    }

    @Override
    public Vertex onDelete(DeletePhysicalRel rel) {
        // currently it's not possible to have a unbounded DELETE, but if we do, we'd need this calculation
        watermarkThrottlingFrameSize = WatermarkThrottlingFrameSizeCalculator.calculate((PhysicalRel) rel.getInput());

        Table table = rel.getTable().unwrap(HazelcastTable.class).getTarget();

        dagBuildContext.setTable(table);
        dagBuildContext.setRel(rel);
        Vertex vertex = getJetSqlConnector(table).deleteProcessor(dagBuildContext);
        connectInput(rel.getInput(), vertex, null);
        return vertex;
    }

    @Override
    public Vertex onFullScan(FullScanPhysicalRel rel) {
        HazelcastTable hazelcastTable = rel.getTable().unwrap(HazelcastTable.class);
        Table table = hazelcastTable.getTarget();
        collectObjectKeys(table);

        BiFunctionEx<ExpressionEvalContext, Byte, EventTimePolicy<JetSqlRow>> policyProvider =
                rel.eventTimePolicyProvider(
                        rel.watermarkedColumnIndex(),
                        rel.lagExpression(),
                        watermarkThrottlingFrameSize);

        Map<Integer, MutableByte> fieldsKey = watermarkKeysAssigner.getWatermarkedFieldsKey(rel);
        Byte wmKey;
        if (fieldsKey != null) {
            wmKey = fieldsKey.get(rel.watermarkedColumnIndex()).getValue();
        } else {
            assert rel.watermarkedColumnIndex() < 0;
            wmKey = null;
        }

        dagBuildContext.setTable(table);
        dagBuildContext.setRel(rel);
        return getJetSqlConnector(table).fullScanReader(
                dagBuildContext,
                CalciteNode.filter(rel.filter()),
                CalciteNode.projection(rel.projection()),
                policyProvider != null
                        ? context -> policyProvider.apply(context, wmKey)
                        : null
        );
    }

    @Override
    public Vertex onMapIndexScan(IndexScanMapPhysicalRel rel) {
        Table table = rel.getTable().unwrap(HazelcastTable.class).getTarget();
        collectObjectKeys(table);

        dagBuildContext.setTable(table);
        dagBuildContext.setRel(rel);
        return SqlConnectorUtil.<IMapSqlConnector>getJetSqlConnector(table)
                .indexScanReader(
                        dagBuildContext,
                        localMemberAddress,
                        rel.getIndex(),
                        CalciteNode.filter(rel.filter()),
                        CalciteNode.projection(rel.projection()),
                        rel.getIndexFilter(),
                        rel.getComparator(),
                        rel.isDescending()
                );
    }

    @Override
    public Vertex onCalc(CalcPhysicalRel rel) {
        RexProgram program = rel.getProgram();
        dagBuildContext.setTable(null);
        dagBuildContext.setRel(rel);
        List<Expression<?>> projection = dagBuildContext.convertProjection(CalciteNode.projection(rel.projection()));

        Vertex vertex;
        if (program.getCondition() != null) {
            Expression<Boolean> filterExpr = dagBuildContext.convertFilter(CalciteNode.filter(rel.filter()));
            vertex = dag.newUniqueVertex("Calc", mapUsingServiceP(
                    ServiceFactories.nonSharedService(ctx ->
                            ExpressionUtil.calcFn(projection, filterExpr, ExpressionEvalContext.from(ctx))),
                    (Function<JetSqlRow, JetSqlRow> calcFn, JetSqlRow row) -> calcFn.apply(row)));
        } else {
            vertex = dag.newUniqueVertex("Project", mapUsingServiceP(
                    ServiceFactories.nonSharedService(ctx ->
                            ExpressionUtil.projectionFn(projection, ExpressionEvalContext.from(ctx))),
                    (Function<JetSqlRow, JetSqlRow> projectionFn, JetSqlRow row) -> projectionFn.apply(row)
            ));
        }
        connectInputPreserveCollation(rel, vertex);
        return vertex;
    }

    @Override
    public Vertex onSort(SortPhysicalRel rel) {
        ComparatorEx<?> comparator = ExpressionUtil.comparisonFn(rel.getCollations());

        // Use 2-Phase sort for maximum parallelism
        // First, construct processors for local sorting
        Vertex sortVertex = dag.newUniqueVertex("Sort",
                ProcessorMetaSupplier.of(sortP(comparator)));
        connectInput(rel.getInput(), sortVertex, null);

        // Then, combine the locally sorted inputs while preserving the ordering
        Vertex combineVertex = dag.newUniqueVertex("SortCombine",
                ProcessorMetaSupplier.forceTotalParallelismOne(
                        ProcessorSupplier.of(mapP(FunctionEx.identity())),
                        localMemberAddress
                )
        );
        Edge edge = between(sortVertex, combineVertex)
                .ordered(comparator)
                .distributeTo(localMemberAddress)
                .allToOne("");
        dag.edge(edge);

        return combineVertex;
    }

    @Override
    public Vertex onAggregate(AggregatePhysicalRel rel) {
        AggregateOperation<?, JetSqlRow> aggregateOperation = rel.aggrOp();

        Vertex vertex = dag.newUniqueVertex(
                "Aggregate",
                ProcessorMetaSupplier.forceTotalParallelismOne(
                        ProcessorSupplier.of(Processors.aggregateP(aggregateOperation)),
                        localMemberAddress
                )
        );
        connectInput(rel.getInput(), vertex, edge -> edge.distributeTo(localMemberAddress).allToOne(""));
        return vertex;
    }

    @Override
    public Vertex onAccumulate(AggregateAccumulatePhysicalRel rel) {
        AggregateOperation<?, JetSqlRow> aggregateOperation = rel.aggrOp();

        Vertex vertex = dag.newUniqueVertex(
                "Accumulate",
                Processors.accumulateP(aggregateOperation)
        );
        connectInput(rel.getInput(), vertex, null);
        return vertex;
    }

    @Override
    public Vertex onCombine(AggregateCombinePhysicalRel rel) {
        AggregateOperation<?, JetSqlRow> aggregateOperation = rel.aggrOp();

        Vertex vertex = dag.newUniqueVertex(
                "Combine",
                ProcessorMetaSupplier.forceTotalParallelismOne(
                        ProcessorSupplier.of(Processors.combineP(aggregateOperation)),
                        localMemberAddress
                )
        );
        connectInput(rel.getInput(), vertex, edge -> edge.distributeTo(localMemberAddress).allToOne(""));
        return vertex;
    }

    @Override
    public Vertex onAggregateByKey(AggregateByKeyPhysicalRel rel) {
        FunctionEx<JetSqlRow, ?> groupKeyFn = rel.groupKeyFn();
        AggregateOperation<?, JetSqlRow> aggregateOperation = rel.aggrOp();

        Vertex vertex = dag.newUniqueVertex(
                "AggregateByKey",
                Processors.aggregateByKeyP(singletonList(groupKeyFn), aggregateOperation, (key, value) -> value)
        );
        connectInput(rel.getInput(), vertex, edge -> edge.distributed().partitioned(groupKeyFn));
        return vertex;
    }

    @Override
    public Vertex onAccumulateByKey(AggregateAccumulateByKeyPhysicalRel rel) {
        FunctionEx<JetSqlRow, ?> groupKeyFn = rel.groupKeyFn();
        AggregateOperation<?, JetSqlRow> aggregateOperation = rel.aggrOp();

        Vertex vertex = dag.newUniqueVertex(
                "AccumulateByKey",
                Processors.accumulateByKeyP(singletonList(groupKeyFn), aggregateOperation)
        );
        connectInput(rel.getInput(), vertex, edge -> edge.partitioned(groupKeyFn));
        return vertex;
    }

    @Override
    public Vertex onCombineByKey(AggregateCombineByKeyPhysicalRel rel) {
        AggregateOperation<?, JetSqlRow> aggregateOperation = rel.aggrOp();

        Vertex vertex = dag.newUniqueVertex(
                "CombineByKey",
                Processors.combineByKeyP(aggregateOperation, (key, value) -> value)
        );
        connectInput(rel.getInput(), vertex, edge -> edge.distributed().partitioned(entryKey()));
        return vertex;
    }

    @Override
    public Vertex onSlidingWindow(SlidingWindowPhysicalRel rel) {
        int orderingFieldIndex = rel.orderingFieldIndex();
        FunctionEx<ExpressionEvalContext, SlidingWindowPolicy> windowPolicySupplier = rel.windowPolicyProvider();

        // this vertex is used only if there's no aggregation by a window bound
        Vertex vertex = dag.newUniqueVertex(
                "Sliding-Window",
                flatMapUsingServiceP(ServiceFactories.nonSharedService(ctx -> {
                            ExpressionEvalContext evalContext = ExpressionEvalContext.from(ctx);
                            SlidingWindowPolicy windowPolicy = windowPolicySupplier.apply(evalContext);
                            return row -> WindowUtils.addWindowBounds(row, orderingFieldIndex, windowPolicy);
                        }),
                        (BiFunctionEx<Function<JetSqlRow, Traverser<JetSqlRow>>, JetSqlRow, Traverser<JetSqlRow>>) Function::apply
                )
        );
        connectInput(rel.getInput(), vertex, null);
        return vertex;
    }

    @Override
    public Vertex onSlidingWindowAggregate(SlidingWindowAggregatePhysicalRel rel) {
        FunctionEx<JetSqlRow, ?> groupKeyFn = rel.groupKeyFn();
        AggregateOperation<?, JetSqlRow> aggregateOperation = rel.aggrOp();

        Expression<?> timestampExpression = rel.timestampExpression();
        ToLongFunctionEx<JetSqlRow> timestampFn = row ->
                WindowUtils.extractMillis(timestampExpression.eval(row.getRow(), MOCK_EEC));
        SlidingWindowPolicy windowPolicy = rel.windowPolicyProvider().apply(MOCK_EEC);

        KeyedWindowResultFunction<? super Object, ? super JetSqlRow, ?> resultMapping =
                rel.outputValueMapping();

        if (rel.numStages() == 1) {
            Vertex vertex = dag.newUniqueVertex(
                    "Sliding-Window-AggregateByKey",
                    Processors.aggregateToSlidingWindowP(
                            singletonList(groupKeyFn),
                            singletonList(timestampFn),
                            TimestampKind.EVENT,
                            windowPolicy,
                            0,
                            aggregateOperation,
                            resultMapping));
            connectInput(rel.getInput(), vertex, edge -> edge.distributeTo(localMemberAddress).allToOne(""));
            return vertex;
        } else {
            assert rel.numStages() == 2;

            Vertex vertex1 = dag.newUniqueVertex(
                    "Sliding-Window-AccumulateByKey",
                    Processors.accumulateByFrameP(
                            singletonList(groupKeyFn),
                            singletonList(timestampFn),
                            TimestampKind.EVENT,
                            windowPolicy,
                            aggregateOperation));

            Vertex vertex2 = dag.newUniqueVertex(
                    "Sliding-Window-CombineByKey",
                    Processors.combineToSlidingWindowP(
                            windowPolicy,
                            aggregateOperation,
                            resultMapping));

            connectInput(rel.getInput(), vertex1, edge -> edge.partitioned(groupKeyFn));
            dag.edge(between(vertex1, vertex2).distributed().partitioned(entryKey()));
            return vertex2;
        }
    }

    @Override
    public Vertex onDropLateItems(DropLateItemsPhysicalRel rel) {
        Expression<?> timestampExpression = rel.timestampExpression();
        byte key = watermarkKeysAssigner.getWatermarkedFieldsKey(rel).get(rel.wmField()).getValue();
        SupplierEx<Processor> lateItemsDropPSupplier = () -> new LateItemsDropP(key, timestampExpression);
        Vertex vertex = dag.newUniqueVertex("Drop-Late-Items", lateItemsDropPSupplier);

        connectInput(rel.getInput(), vertex, null);
        return vertex;
    }

    public Vertex onNestedLoopJoin(JoinNestedLoopPhysicalRel rel) {
        assert rel.getRight() instanceof HazelcastPhysicalScan : rel.getRight().getClass();

        Table rightTable = rel.getRight().getTable().unwrap(HazelcastTable.class).getTarget();
        collectObjectKeys(rightTable);

        dagBuildContext.setTable(rightTable);
        dagBuildContext.setRel(rel);
        VertexWithInputConfig vertexWithConfig = getJetSqlConnector(rightTable).nestedLoopReader(
                dagBuildContext,
                CalciteNode.filter(rel.rightFilter()),
                CalciteNode.projection(rel.rightProjection()),
                rel.joinInfo(dagBuildContext.getParameterMetadata())
        );
        Vertex vertex = vertexWithConfig.vertex();
        connectInput(rel.getLeft(), vertex, vertexWithConfig.configureEdgeFn());
        return vertex;
    }

    @Override
    public Vertex onHashJoin(JoinHashPhysicalRel rel) {
        JetJoinInfo joinInfo = rel.joinInfo(dagBuildContext.getParameterMetadata());

        Vertex joinVertex = dag.newUniqueVertex(
                "Hash Join",
                SqlHashJoinP.supplier(
                        joinInfo,
                        rel.getRight().getRowType().getFieldCount()
                )
        );
        connectJoinInput(joinInfo, rel.getLeft(), rel.getRight(), joinVertex);
        return joinVertex;
    }

    @Override
    public Vertex onStreamToStreamJoin(StreamToStreamJoinPhysicalRel rel) {
        JetJoinInfo joinInfo = rel.joinInfo(dagBuildContext.getParameterMetadata());

        Map<Byte, ToLongFunctionEx<JetSqlRow>> leftExtractors = new HashMap<>();
        Map<Byte, ToLongFunctionEx<JetSqlRow>> rightExtractors = new HashMap<>();

        // map watermarked timestamps extractors to enumerated wm keys
        Map<Integer, MutableByte> refByteMap = watermarkKeysAssigner.getWatermarkedFieldsKey(rel.getLeft());
        for (Map.Entry<Integer, ToLongFunctionEx<JetSqlRow>> e : rel.leftTimeExtractors().entrySet()) {
            Byte wmKey = refByteMap.get(e.getKey()).getValue();
            leftExtractors.put(wmKey, e.getValue());
        }

        refByteMap = watermarkKeysAssigner.getWatermarkedFieldsKey(rel.getRight());
        for (Map.Entry<Integer, ToLongFunctionEx<JetSqlRow>> e : rel.rightTimeExtractors().entrySet()) {
            Byte wmKey = refByteMap.get(e.getKey()).getValue();
            rightExtractors.put(wmKey, e.getValue());
        }

        // map field descriptors to enumerated watermark keys
        refByteMap = watermarkKeysAssigner.getWatermarkedFieldsKey(rel);
        Map<Byte, Map<Byte, Long>> postponeTimeMap = new HashMap<>();
        for (Entry<Integer, Map<Integer, Long>> entry : rel.postponeTimeMap().entrySet()) {
            Map<Byte, Long> map = new HashMap<>();
            for (Entry<Integer, Long> innerEntry : entry.getValue().entrySet()) {
                map.put(refByteMap.get(innerEntry.getKey()).getValue(), innerEntry.getValue());
            }
            postponeTimeMap.put(refByteMap.get(entry.getKey()).getValue(), map);
        }

        // fill `postponeTimeMap` with empty inner maps for unused
        // watermarks keys to be counted by the processor as present.
        for (MutableByte key : refByteMap.values()) {
            postponeTimeMap.putIfAbsent(key.getValue(), emptyMap());
        }

        Vertex joinVertex = dag.newUniqueVertex(
                "Stream-Stream Join",
                new StreamToStreamJoinProcessorSupplier(
                        joinInfo,
                        leftExtractors,
                        rightExtractors,
                        postponeTimeMap,
                        rel.getLeft().getRowType().getFieldCount(),
                        rel.getRight().getRowType().getFieldCount()));

        connectStreamToStreamJoinInput(joinInfo, rel.getLeft(), rel.getRight(), joinVertex);

        return joinVertex;
    }

    @Override
    public Vertex onUnion(UnionPhysicalRel rel) {
        // Union[all=false] rel should be never be produced, and it is always replaced by
        // UNION_TO_DISTINCT rule : Union[all=false] -> Union[all=true] + Aggregate.
        if (!rel.all) {
            throw new RuntimeException("Union[all=false] rel should never be produced");
        }

        Vertex merger = dag.newUniqueVertex(
                "UnionMerger",
                ProcessorSupplier.of(mapP(FunctionEx.identity()))
        );

        int ordinal = 0;
        for (RelNode input : rel.getInputs()) {
            Vertex inputVertex = ((PhysicalRel) input).accept(this);
            Edge edge = Edge.from(inputVertex).to(merger, ordinal++);
            dag.edge(edge);
        }
        return merger;
    }

    @Override
    public Vertex onLimit(LimitPhysicalRel rel) {
        throw QueryException.error("FETCH/OFFSET is only supported for the top-level SELECT");
    }

    @Override
    public Vertex onRoot(RootRel rootRel) {
        watermarkThrottlingFrameSize = WatermarkThrottlingFrameSizeCalculator.calculate((PhysicalRel) rootRel.getInput());

        RelNode input = rootRel.getInput();

        Expression<?> fetch = ConstantExpression.create(Long.MAX_VALUE, QueryDataType.BIGINT);
        Expression<?> offset = ConstantExpression.create(0L, QueryDataType.BIGINT);

        // We support only top-level LIMIT ... OFFSET.
        if (input instanceof LimitPhysicalRel) {
            LimitPhysicalRel limit = (LimitPhysicalRel) input;
            if (limit.fetch() != null) {
                fetch = limit.fetch(dagBuildContext.getParameterMetadata());
            }

            if (limit.offset() != null) {
                offset = limit.offset(dagBuildContext.getParameterMetadata());
            }
            input = limit.getInput();
        }

        Vertex vertex = dag.newUniqueVertex(
                "ClientSink",
                rootResultConsumerSink(localMemberAddress, fetch, offset)
        );

        // We use distribute-to-one edge to send all the items to the initiator member.
        // Such edge has to be partitioned, but the sink is LP=1 anyway, so we can use
        // allToOne with any key, it goes to a single processor on a single member anyway.
        connectInput(input, vertex, edge -> edge.distributeTo(localMemberAddress).allToOne(""));
        return vertex;
    }

    public void optimizeFinishedDag() {
        decreaseParallelism(dag, nodeEngine.getConfig().getJetConfig().getCooperativeThreadCount());
    }

    // package-visible for test
    static void decreaseParallelism(DAG dag, int defaultParallelism) {
        if (defaultParallelism == 1) {
            return;
        }

        Set<Vertex> verticesToChangeParallelism = new HashSet<>();
        for (Vertex vertex : dag) {
            for (Edge edge : dag.getInboundEdges(vertex.getName())) {
                if (shouldChangeLocalParallelism(edge) && edge.isLocal()) {
                    verticesToChangeParallelism.add(edge.getSource());
                    verticesToChangeParallelism.add(edge.getDestination());
                    edge.isolated();
                }
            }
        }

        int newParallelism = (int) Math.max(2, Math.sqrt(defaultParallelism));
        verticesToChangeParallelism.forEach(vertex -> {
            if (vertex.getMetaSupplier().preferredLocalParallelism() == LOCAL_PARALLELISM_USE_DEFAULT) {
                vertex.localParallelism(newParallelism);
            }
        });
    }

    private static boolean shouldChangeLocalParallelism(Edge edge) {
        if (edge.getDestination() == null) {
            return false;
        }
        return edge.getSource().getLocalParallelism() == LOCAL_PARALLELISM_USE_DEFAULT &&
                edge.getDestination().getLocalParallelism() == LOCAL_PARALLELISM_USE_DEFAULT;
    }

    public Set<PlanObjectKey> getObjectKeys() {
        return objectKeys;
    }

    /**
     * Converts the {@code inputRel} into a {@code Vertex} by visiting it and
     * create an edge from the input vertex into {@code thisVertex}.
     *
     * @param configureEdgeFn optional function to configure the edge
     * @return the input vertex
     */
    private Vertex connectInput(
            RelNode inputRel,
            Vertex thisVertex,
            @Nullable Consumer<Edge> configureEdgeFn
    ) {
        Vertex inputVertex = ((PhysicalRel) inputRel).accept(this);
        Edge edge = between(inputVertex, thisVertex);
        if (configureEdgeFn != null) {
            configureEdgeFn.accept(edge);
        }
        dag.edge(edge);
        return inputVertex;
    }

    private void connectJoinInput(
            JetJoinInfo joinInfo,
            RelNode leftInputRel,
            RelNode rightInputRel,
            Vertex joinVertex
    ) {
        Vertex leftInput = ((PhysicalRel) leftInputRel).accept(this);
        Vertex rightInput = ((PhysicalRel) rightInputRel).accept(this);

        Edge left = between(leftInput, joinVertex).priority(LOW_PRIORITY).broadcast().distributed();
        Edge right = from(rightInput).to(joinVertex, 1).priority(HIGH_PRIORITY).unicast().local();
        if (joinInfo.isLeftOuter()) {
            left = left.unicast().local();
            right = right.broadcast().distributed();
        }
        if (joinInfo.isEquiJoin()) {
            left = left.distributed().partitioned(ObjectArrayKey.projectFn(joinInfo.leftEquiJoinIndices()));
            right = right.distributed().partitioned(ObjectArrayKey.projectFn(joinInfo.rightEquiJoinIndices()));
        }
        dag.edge(left);
        dag.edge(right);
    }

    private void connectStreamToStreamJoinInput(
            JetJoinInfo joinInfo,
            RelNode leftInputRel,
            RelNode rightInputRel,
            Vertex joinVertex
    ) {
        Vertex leftInput = ((PhysicalRel) leftInputRel).accept(this);
        Vertex rightInput = ((PhysicalRel) rightInputRel).accept(this);

        Edge left = Edge.from(leftInput).to(joinVertex, 0);
        Edge right = Edge.from(rightInput).to(joinVertex, 1);

        if (joinInfo.isRightOuter()) {
            left = left.distributed().broadcast();
            right = right.unicast().local();
        } else {
            // this strategy applies to left and inner joins non-equi joins
            left = left.unicast().local();
            right = right.distributed().broadcast();
        }

        if (joinInfo.isEquiJoin()) {
            left = left.distributed().partitioned(ObjectArrayKey.projectFn(joinInfo.leftEquiJoinIndices()));
            right = right.distributed().partitioned(ObjectArrayKey.projectFn(joinInfo.rightEquiJoinIndices()));
        }

        dag.edge(left);
        dag.edge(right);
    }

    /**
     * Same as {@link #connectInput(RelNode, Vertex, Consumer)}, but used for
     * vertices normally connected by an unicast or isolated edge, depending on
     * whether the {@code rel} has collation fields.
     *
     * @param rel    The rel to connect to input
     * @param vertex The vertex for {@code rel}
     */
    private void connectInputPreserveCollation(SingleRel rel, Vertex vertex) {
        boolean preserveCollation = rel.getTraitSet().getCollation().getFieldCollations().size() > 0;
        Vertex inputVertex = connectInput(rel.getInput(), vertex,
                preserveCollation ? Edge::isolated : null);

        if (preserveCollation) {
            int cooperativeThreadCount = nodeEngine.getConfig().getJetConfig().getCooperativeThreadCount();
            int explicitLP = inputVertex.determineLocalParallelism(cooperativeThreadCount);
            // It's not strictly necessary to set the LP to the input,
            // but we do it to ensure that the two vertices indeed have the same LP
            inputVertex.determineLocalParallelism(explicitLP);
            vertex.localParallelism(explicitLP);
        }
    }

    private void collectObjectKeys(Table table) {
        PlanObjectKey objectKey = table.getObjectKey();
        if (objectKey != null) {
            objectKeys.add(objectKey);
        }
    }
}
