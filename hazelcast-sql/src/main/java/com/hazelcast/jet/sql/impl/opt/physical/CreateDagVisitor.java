/*
 * Copyright 2021 Hazelcast Inc.
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
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
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
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.ObjectArrayKey;
import com.hazelcast.jet.sql.impl.aggregate.WindowUtils;
import com.hazelcast.jet.sql.impl.connector.SqlConnector.VertexWithInputConfig;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.jet.sql.impl.opt.ExpressionValues;
import com.hazelcast.jet.sql.impl.processors.LateItemsDropP;
import com.hazelcast.jet.sql.impl.processors.SqlHashJoinP;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.spi.impl.NodeEngine;
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
import java.util.HashSet;
import java.util.List;
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
import static java.util.Collections.singletonList;

public class CreateDagVisitor {

    // TODO https://github.com/hazelcast/hazelcast/issues/20383
    private static final ExpressionEvalContext MOCK_EEC =
            new ExpressionEvalContext(emptyList(), new DefaultSerializationServiceBuilder().build());

    private static final int LOW_PRIORITY = 10;
    private static final int HIGH_PRIORITY = 1;

    private final DAG dag = new DAG();
    private final Set<PlanObjectKey> objectKeys = new HashSet<>();
    private final NodeEngine nodeEngine;
    private final Address localMemberAddress;
    private final QueryParameterMetadata parameterMetadata;

    public CreateDagVisitor(NodeEngine nodeEngine, QueryParameterMetadata parameterMetadata) {
        this.nodeEngine = nodeEngine;
        this.localMemberAddress = nodeEngine.getThisAddress();
        this.parameterMetadata = parameterMetadata;
    }

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

    public Vertex onInsert(InsertPhysicalRel rel) {
        Table table = rel.getTable().unwrap(HazelcastTable.class).getTarget();
        collectObjectKeys(table);

        VertexWithInputConfig vertexWithConfig = getJetSqlConnector(table).insertProcessor(dag, table);
        Vertex vertex = vertexWithConfig.vertex();
        connectInput(rel.getInput(), vertex, vertexWithConfig.configureEdgeFn());
        return vertex;
    }

    public Vertex onSink(SinkPhysicalRel rel) {
        Table table = rel.getTable().unwrap(HazelcastTable.class).getTarget();
        collectObjectKeys(table);

        Vertex vertex = getJetSqlConnector(table).sinkProcessor(dag, table);
        connectInput(rel.getInput(), vertex, null);
        return vertex;
    }

    public Vertex onUpdate(UpdatePhysicalRel rel) {
        Table table = rel.getTable().unwrap(HazelcastTable.class).getTarget();

        Vertex vertex = getJetSqlConnector(table).updateProcessor(dag, table, rel.updates(parameterMetadata));
        connectInput(rel.getInput(), vertex, null);
        return vertex;
    }

    public Vertex onDelete(DeletePhysicalRel rel) {
        Table table = rel.getTable().unwrap(HazelcastTable.class).getTarget();

        Vertex vertex = getJetSqlConnector(table).deleteProcessor(dag, table);
        connectInput(rel.getInput(), vertex, null);
        return vertex;
    }

    public Vertex onFullScan(FullScanPhysicalRel rel) {
        Table table = rel.getTable().unwrap(HazelcastTable.class).getTarget();
        collectObjectKeys(table);

        return getJetSqlConnector(table).fullScanReader(
                dag,
                table,
                rel.filter(parameterMetadata),
                rel.projection(parameterMetadata),
                rel.eventTimePolicyProvider()
        );
    }

    public Vertex onMapIndexScan(IndexScanMapPhysicalRel rel) {
        Table table = rel.getTable().unwrap(HazelcastTable.class).getTarget();
        collectObjectKeys(table);

        return SqlConnectorUtil.<IMapSqlConnector>getJetSqlConnector(table)
                .indexScanReader(
                        dag,
                        localMemberAddress,
                        table,
                        rel.getIndex(),
                        rel.filter(parameterMetadata),
                        rel.projection(parameterMetadata),
                        rel.getIndexFilter(),
                        rel.getComparator(),
                        rel.isDescending()
                );
    }

    public Vertex onCalc(CalcPhysicalRel rel) {
        RexProgram program = rel.getProgram();
        List<Expression<?>> projection = rel.projection(parameterMetadata);

        Vertex vertex;
        if (program.getCondition() != null) {
            Expression<Boolean> filterExpr = rel.filter(parameterMetadata);

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

    public Vertex onAccumulate(AggregateAccumulatePhysicalRel rel) {
        AggregateOperation<?, JetSqlRow> aggregateOperation = rel.aggrOp();

        Vertex vertex = dag.newUniqueVertex(
                "Accumulate",
                Processors.accumulateP(aggregateOperation)
        );
        connectInput(rel.getInput(), vertex, null);
        return vertex;
    }

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

    public Vertex onCombineByKey(AggregateCombineByKeyPhysicalRel rel) {
        AggregateOperation<?, JetSqlRow> aggregateOperation = rel.aggrOp();

        Vertex vertex = dag.newUniqueVertex(
                "CombineByKey",
                Processors.combineByKeyP(aggregateOperation, (key, value) -> value)
        );
        connectInput(rel.getInput(), vertex, edge -> edge.distributed().partitioned(entryKey()));
        return vertex;
    }

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

    public Vertex onDropLateItems(DropLateItemsPhysicalRel rel) {
        Expression<?> timestampExpression = rel.timestampExpression();

        SupplierEx<Processor> lateItemsDropPSupplier = () -> new LateItemsDropP(timestampExpression);
        Vertex vertex = dag.newUniqueVertex("Drop-Late-Items", lateItemsDropPSupplier);

        connectInput(rel.getInput(), vertex, null);
        return vertex;
    }

    public Vertex onNestedLoopJoin(JoinNestedLoopPhysicalRel rel) {
        assert rel.getRight() instanceof FullScanPhysicalRel : rel.getRight().getClass();

        Table rightTable = rel.getRight().getTable().unwrap(HazelcastTable.class).getTarget();
        collectObjectKeys(rightTable);

        VertexWithInputConfig vertexWithConfig = getJetSqlConnector(rightTable).nestedLoopReader(
                dag,
                rightTable,
                rel.rightFilter(parameterMetadata),
                rel.rightProjection(parameterMetadata),
                rel.joinInfo(parameterMetadata)
        );
        Vertex vertex = vertexWithConfig.vertex();
        connectInput(rel.getLeft(), vertex, vertexWithConfig.configureEdgeFn());
        return vertex;
    }

    public Vertex onHashJoin(JoinHashPhysicalRel rel) {
        JetJoinInfo joinInfo = rel.joinInfo(parameterMetadata);

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

    public Vertex onRoot(RootRel rootRel) {
        RelNode input = rootRel.getInput();
        Expression<?> fetch;
        Expression<?> offset;

        if (input instanceof SortPhysicalRel || isCalcWithSort(input)) {
            SortPhysicalRel sortRel = input instanceof SortPhysicalRel
                    ? (SortPhysicalRel) input
                    : (SortPhysicalRel) ((CalcPhysicalRel) input).getInput();

            if (sortRel.fetch == null) {
                fetch = ConstantExpression.create(Long.MAX_VALUE, QueryDataType.BIGINT);
            } else {
                fetch = sortRel.fetch(parameterMetadata);
            }

            if (sortRel.offset == null) {
                offset = ConstantExpression.create(0L, QueryDataType.BIGINT);
            } else {
                offset = sortRel.offset(parameterMetadata);
            }

            if (!sortRel.requiresSort()) {
                input = sortRel.getInput();
            }
        } else {
            fetch = ConstantExpression.create(Long.MAX_VALUE, QueryDataType.BIGINT);
            offset = ConstantExpression.create(0L, QueryDataType.BIGINT);
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

    public DAG getDag() {
        return dag;
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

    private boolean isCalcWithSort(RelNode input) {
        return input instanceof CalcPhysicalRel &&
                ((CalcPhysicalRel) input).getInput() instanceof SortPhysicalRel;
    }
}
