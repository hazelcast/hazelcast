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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexType;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataJavaResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolvers;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvProcessors;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvProjector;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.opt.physical.CreateDagVisitor;
import com.hazelcast.jet.sql.impl.opt.physical.CreateDagVisitorBase;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.IndexScanMapPhysicalRel;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.exec.scan.MapIndexScanMetadata;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.row.JetSqlJoinRow;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.impl.schema.map.MapTableUtils;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUuidString;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.updateMapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeMapP;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.sql.impl.connector.map.MapIndexScanP.readMapIndexSupplier;
import static com.hazelcast.jet.sql.impl.connector.map.RowProjectorProcessorSupplier.rowProjector;
import static com.hazelcast.sql.impl.schema.map.MapTableUtils.estimatePartitionedMapRowCount;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

public class IMapSqlConnector implements SqlConnector {

    public static final IMapSqlConnector INSTANCE = new IMapSqlConnector();

    public static final String TYPE_NAME = "IMap";
    public static final List<String> PRIMARY_KEY_LIST = singletonList(QueryPath.KEY);

    private static final KvMetadataResolvers METADATA_RESOLVERS_WITH_COMPACT = new KvMetadataResolvers(
            KvMetadataJavaResolver.INSTANCE,
            MetadataPortableResolver.INSTANCE,
            MetadataCompactResolver.INSTANCE,
            MetadataJsonResolver.INSTANCE
    );

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Override
    public boolean isStream() {
        return false;
    }

    @Nonnull
    @Override
    public List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> userFields,
            @Nonnull String externalName
    ) {
        return METADATA_RESOLVERS_WITH_COMPACT.resolveAndValidateFields(userFields, options, nodeEngine);
    }

    @Nonnull
    @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String mappingName,
            @Nonnull String externalName,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> resolvedFields
    ) {
        InternalSerializationService ss = (InternalSerializationService) nodeEngine.getSerializationService();

        KvMetadata keyMetadata = METADATA_RESOLVERS_WITH_COMPACT.resolveMetadata(
                true,
                resolvedFields,
                options, ss
        );
        KvMetadata valueMetadata = METADATA_RESOLVERS_WITH_COMPACT.resolveMetadata(
                false,
                resolvedFields,
                options,
                ss
        );
        List<TableField> fields = concat(keyMetadata.getFields().stream(), valueMetadata.getFields().stream())
                .collect(toList());

        MapService service = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext context = service.getMapServiceContext();
        MapContainer container = context.getExistingMapContainer(externalName);

        long estimatedRowCount = estimatePartitionedMapRowCount(nodeEngine, context, externalName);
        boolean hd = container != null && container.getMapConfig().getInMemoryFormat() == InMemoryFormat.NATIVE;
        List<MapTableIndex> indexes = container != null
                ? MapTableUtils.getPartitionedMapIndexes(container, fields)
                : emptyList();

        return new PartitionedMapTable(
                schemaName,
                mappingName,
                externalName,
                fields,
                new ConstantTableStatistics(estimatedRowCount),
                keyMetadata.getQueryTargetDescriptor(),
                valueMetadata.getQueryTargetDescriptor(),
                keyMetadata.getUpsertTargetDescriptor(),
                valueMetadata.getUpsertTargetDescriptor(),
                indexes,
                hd
        );
    }

    @Nonnull
    @Override
    public Vertex fullScanReader(
            @Nonnull DAG dag,
            @Nonnull Table table0,
            @Nullable Expression<Boolean> filter,
            @Nonnull List<Expression<?>> projection,
            @Nullable FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider
    ) {
        if (eventTimePolicyProvider != null) {
            throw QueryException.error("Ordering functions are not supported on top of " + TYPE_NAME + " mappings");
        }

        PartitionedMapTable table = (PartitionedMapTable) table0;

        Vertex vStart = dag.newUniqueVertex(
                toString(table),
                SourceProcessors.readMapP(table.getMapName())
        );

        Vertex vEnd = dag.newUniqueVertex(
                "Project(" + toString(table) + ")",
                rowProjector(
                        table.paths(),
                        table.types(),
                        table.getKeyDescriptor(),
                        table.getValueDescriptor(),
                        filter,
                        projection
                )
        );

        dag.edge(Edge.from(vStart).to(vEnd).isolated());
        return vEnd;
    }

    /**
     * @return tuple of (input vertex, output vertex). Input vertex is null for
     *         source mode (not join mode). Both elements can be the same vertex.
     */
    @Nonnull
    @SuppressWarnings("checkstyle:ParameterNumber")
    public Tuple2<VertexWithInputConfig, Vertex> indexScanReader(
            @Nonnull DAG dag,
            @Nonnull Address localMemberAddress,
            @Nonnull Table table0,
            @Nonnull MapTableIndex tableIndex,
            @Nullable Expression<Boolean> remainingFilter,
            @Nonnull List<Expression<?>> projection,
            @Nullable IndexFilter indexFilter,
            @Nullable ComparatorEx<JetSqlRow> comparator,
            boolean descending,
            @Nullable JetJoinInfo joinInfo
    ) {
        PartitionedMapTable table = (PartitionedMapTable) table0;
        MapIndexScanMetadata indexScanMetadata = new MapIndexScanMetadata(
                table.getMapName(),
                tableIndex.getName(),
                table.getKeyDescriptor(),
                table.getValueDescriptor(),
                Arrays.asList(table.paths()),
                Arrays.asList(table.types()),
                indexFilter,
                projection,
                remainingFilter,
                comparator,
                descending,
                joinInfo
        );

        Vertex scanner = dag.newUniqueVertex(
                (joinInfo != null ? "Join(" : "") + "Index(" + toString(table) + ")"
                    + (joinInfo != null ? ")" : ""),
                readMapIndexSupplier(indexScanMetadata)
        );

        if (joinInfo == null) {
            // LP must be 1 in scan mode - one local index contains all local
            // partitions, if there are 2 local processors, the index will be
            // scanned twice and each time half of the partitions will be
            // thrown out.
            // It is not the case in join mode were we scan the partitions
            // repeatedly, once for each left item.
            scanner.localParallelism(1);
        }

        if (joinInfo != null && !joinInfo.isInner()) {
            // TODO: snapshots and watermarks support in ordered edges

            // Parallelism for idAssigner and merger should be the same
            // so the items will be properly collected to partitions in merger.
            // We could have fewer idAssigners than mergers if each idAssigner
            // generated keys for non-overlapping partitions, but not the other
            // way around.
            //
            // Currently, the parallelism is limited to 1 because ordered
            // partitioned edge does not handle case when items are ordered
            // only within partition/processor instance, they must have total
            // order. It is caused by the fact that there is a single
            // SenderTasklet that sends data to remote node even if there is >1
            // processor instance there. SenderTasklet uses single OrderedDrain
            // for all incoming data which mixes different partitions, in
            // particular partitions assigned to different processor instances.
            Vertex idAssigner = dag.newUniqueVertex(
                    "OuterNestedLoopIdAssigner",
                    ProcessorSupplier.of(IdAssignerProcessor::new)
            ).localParallelism(1);

            dag.edge(between(idAssigner, scanner)
                    // send left items to all nodes, so they can process their local partitions
                    .distributed()
                    // each row needs to be processed by one scanner instance on each node
                    // each scanner scans all local partitions
                    .fanout());

            Vertex merger = dag.newUniqueVertex(
                    "OuterNestedLoopMerger",
                    // TODO: convert JetSqlJoinRow to JetSqlRow (or maybe not needed?)
                    ProcessorSupplier.of(MergerProcessor::new)
            ).localParallelism(1);

            dag.edge(between(scanner, merger)
                    .distributed()
                    .partitioned(JetSqlJoinRow::getProcessorIndex, (index, count) -> {
                        // explicitly partition in the same way as ids were assigned
                        // so in each partition rowId is monotonic
                        assert index <= count;
                        return index;
                    })
                    .ordered(ComparatorEx.comparing(JetSqlJoinRow::getRowId))
            );

            return tuple2(new VertexWithInputConfig(idAssigner, edge -> {
                // id assigner will distribute rows
                edge.local().isolated();
            }), merger);
        }

        // TODO: why do we sort always IndexType.SORTED? The query may not require it
        if (joinInfo == null && tableIndex.getType() == IndexType.SORTED) {
            Vertex sorter = dag.newUniqueVertex(
                    "SortCombine",
                    ProcessorMetaSupplier.forceTotalParallelismOne(
                            ProcessorSupplier.of(mapP(FunctionEx.identity())),
                            localMemberAddress
                    )
            );

            assert comparator != null;
            dag.edge(between(scanner, sorter)
                    .ordered(comparator)
                    .distributeTo(localMemberAddress)
                    .allToOne("")
            );
            return tuple2(null, sorter);
        }

        if (joinInfo == null) {
            return tuple2(null, scanner);
        } else {
            // single stage join (inner)
            return tuple2(new VertexWithInputConfig(scanner, edge -> {
                // send left items to all nodes, so they can process their local partitions
                edge.distributed().fanout();
            }), scanner);
        }
    }

    @Nonnull
    @Override
    public CreateDagVisitor<Tuple2<VertexWithInputConfig, VertexWithInputConfig>> nestedLoopReader(
            @Nonnull DAG dag,
            @Nonnull Table table0,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projections,
            @Nonnull JetJoinInfo joinInfo,
            @Nonnull CreateDagVisitor<Vertex> parentVisitor
    ) {
        PartitionedMapTable table = (PartitionedMapTable) table0;
        String mapName = table.getMapName();
        String tableName = toString(table);

        KvRowProjector.Supplier rightRowProjectorSupplier = KvRowProjector.supplier(
                table.paths(),
                table.types(),
                table.getKeyDescriptor(),
                table.getValueDescriptor(),
                predicate,
                projections
        );

        @SuppressWarnings("checkstyle:anoninnerlength")
        CreateDagVisitor<Tuple2<VertexWithInputConfig, VertexWithInputConfig>> visitor
                = new CreateDagVisitorBase<Tuple2<VertexWithInputConfig, VertexWithInputConfig>>(dag,
                    parentVisitor.getLocalMemberAddress(), parentVisitor.getParameterMetadata()) {
            // TODO: support key lookup
            @Override
            public Tuple2<VertexWithInputConfig, VertexWithInputConfig> onMapIndexScan(IndexScanMapPhysicalRel scanRel) {
                // IndexScanMapPhysicalRel for IMap is correlation-variable-aware
                // TODO: collect tables/keys!

                // index scan is supported only for IMap
                // f0 - input, f1 - output
                Tuple2<VertexWithInputConfig, Vertex> vertices = indexScanReader(
                        dag,
                        localMemberAddress,
                        table,
                        scanRel.getIndex(),
                        scanRel.filter(parameterMetadata),
                        scanRel.projection(parameterMetadata),
                        scanRel.getIndexFilter(),
                        scanRel.getComparator(),
                        scanRel.isDescending(),
                        joinInfo
                );

                // TODO: support partition-aligned joins
                return tuple2(vertices.f0(),
                        //TODO: config for output?
                        new VertexWithInputConfig(vertices.f1()));
            }

            @Override
            public Tuple2<VertexWithInputConfig, VertexWithInputConfig> onFullScan(FullScanPhysicalRel rel) {
                VertexWithInputConfig vertex = new VertexWithInputConfig(
                        dag.newUniqueVertex(
                                "Join(Scan-" + tableName + ")",
                                new JoinScanProcessorSupplier(joinInfo, mapName, rightRowProjectorSupplier)
                        )
                );
                return tuple2(vertex, vertex);
            }
        };
        return visitor;
    }

    @Nonnull
    @Override
    public VertexWithInputConfig insertProcessor(
            @Nonnull DAG dag,
            @Nonnull Table table0
    ) {
        PartitionedMapTable table = (PartitionedMapTable) table0;

        Vertex vertex = dag.newUniqueVertex(
                toString(table),
                new InsertProcessorSupplier(
                        table.getMapName(),
                        KvProjector.supplier(
                                table.paths(),
                                table.types(),
                                (UpsertTargetDescriptor) table.getKeyJetMetadata(),
                                (UpsertTargetDescriptor) table.getValueJetMetadata(),
                                true
                        )
                )
        ).localParallelism(1);
        return new VertexWithInputConfig(vertex, edge -> edge.distributed().allToOne(newUnsecureUuidString()));
    }

    @Nonnull
    @Override
    public Vertex sinkProcessor(
            @Nonnull DAG dag,
            @Nonnull Table table0
    ) {
        PartitionedMapTable table = (PartitionedMapTable) table0;

        Vertex vStart = dag.newUniqueVertex(
                "Project(" + toString(table) + ")",
                KvProcessors.entryProjector(
                        table.paths(),
                        table.types(),
                        (UpsertTargetDescriptor) table.getKeyJetMetadata(),
                        (UpsertTargetDescriptor) table.getValueJetMetadata(),
                        true
                )
        );

        Vertex vEnd = dag.newUniqueVertex(
                toString(table),
                writeMapP(table.getMapName())
        );

        dag.edge(between(vStart, vEnd));
        return vStart;
    }

    @Nonnull
    @Override
    public Vertex updateProcessor(
            @Nonnull DAG dag,
            @Nonnull Table table0,
            @Nonnull Map<String, Expression<?>> updatesByFieldNames
    ) {
        PartitionedMapTable table = (PartitionedMapTable) table0;

        return dag.newUniqueVertex(
                "Update(" + toString(table) + ")",
                new UpdateProcessorSupplier(
                        table.getMapName(),
                        UpdatingEntryProcessor.supplier(table, updatesByFieldNames)
                )
        );
    }

    @Nonnull
    @Override
    public Vertex deleteProcessor(@Nonnull DAG dag, @Nonnull Table table0) {
        PartitionedMapTable table = (PartitionedMapTable) table0;

        return dag.newUniqueVertex(
                toString(table),
                // TODO do a simpler, specialized deleting-only processor
                updateMapP(table.getMapName(), (FunctionEx<JetSqlRow, Object>) row -> {
                    assert row.getFieldCount() == 1;
                    return row.get(0);
                }, (v, t) -> null));
    }

    @Nonnull
    @Override
    public List<String> getPrimaryKey(Table table0) {
        return PRIMARY_KEY_LIST;
    }

    private static String toString(PartitionedMapTable table) {
        return TYPE_NAME + "[" + table.getSchemaName() + "." + table.getSqlName() + "]";
    }
}
