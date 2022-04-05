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
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataJavaResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolvers;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvProcessors;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvProjector;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
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

    private static final KvMetadataResolvers METADATA_RESOLVERS_WITHOUT_COMPACT = new KvMetadataResolvers(
            KvMetadataJavaResolver.INSTANCE,
            MetadataPortableResolver.INSTANCE,
            MetadataCompactDisabledResolver.INSTANCE,
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
            @Nonnull List<MappingField> userFields
    ) {
        if (nodeEngine.getConfig().getSerializationConfig().getCompactSerializationConfig().isEnabled()) {
            return METADATA_RESOLVERS_WITH_COMPACT.resolveAndValidateFields(userFields, options, nodeEngine);
        } else {
            return METADATA_RESOLVERS_WITHOUT_COMPACT.resolveAndValidateFields(userFields, options, nodeEngine);
        }
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

        KvMetadata keyMetadata = METADATA_RESOLVERS_WITH_COMPACT.resolveMetadata(true, resolvedFields, options, ss);
        KvMetadata valueMetadata = METADATA_RESOLVERS_WITH_COMPACT.resolveMetadata(false, resolvedFields, options, ss);
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

    @Nonnull
    @SuppressWarnings("checkstyle:ParameterNumber")
    public Vertex indexScanReader(
            @Nonnull DAG dag,
            @Nonnull Address localMemberAddress,
            @Nonnull Table table0,
            @Nonnull MapTableIndex tableIndex,
            @Nullable Expression<Boolean> remainingFilter,
            @Nonnull List<Expression<?>> projection,
            @Nullable IndexFilter indexFilter,
            @Nullable ComparatorEx<JetSqlRow> comparator,
            boolean descending
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
                descending
        );

        Vertex scanner = dag.newUniqueVertex(
                "Index(" + toString(table) + ")",
                readMapIndexSupplier(indexScanMetadata)
        );
        // LP must be 1 - one local index contains all local partitions, if there are 2 local processors,
        // the index will be scanned twice and each time half of the partitions will be thrown out.
        scanner.localParallelism(1);

        if (tableIndex.getType() == IndexType.SORTED) {
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
            return sorter;
        }
        return scanner;
    }

    @Nonnull
    @Override
    public VertexWithInputConfig nestedLoopReader(
            @Nonnull DAG dag,
            @Nonnull Table table0,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projections,
            @Nonnull JetJoinInfo joinInfo
    ) {
        PartitionedMapTable table = (PartitionedMapTable) table0;

        KvRowProjector.Supplier rightRowProjectorSupplier = KvRowProjector.supplier(
                table.paths(),
                table.types(),
                table.getKeyDescriptor(),
                table.getValueDescriptor(),
                predicate,
                projections
        );

        return Joiner.join(dag, table.getMapName(), toString(table), joinInfo, rightRowProjectorSupplier);
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
