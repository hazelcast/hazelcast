/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataJavaResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolvers;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvProcessors;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvProjector;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.plan.node.MapScanMetadata;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.SinkProcessors.updateMapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeMapP;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY_PATH;
import static com.hazelcast.sql.impl.schema.map.MapTableUtils.estimatePartitionedMapRowCount;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

public class IMapSqlConnector implements SqlConnector {

    public static final IMapSqlConnector INSTANCE = new IMapSqlConnector();

    public static final String TYPE_NAME = "IMap";

    private static final KvMetadataResolvers METADATA_RESOLVERS = new KvMetadataResolvers(
            KvMetadataJavaResolver.INSTANCE,
            MetadataPortableResolver.INSTANCE,
            MetadataJsonResolver.INSTANCE
    );

    private static final List<String> PRIMARY_KEY_LIST = singletonList(QueryPath.KEY);

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
        return METADATA_RESOLVERS.resolveAndValidateFields(userFields, options, nodeEngine);
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

        KvMetadata keyMetadata = METADATA_RESOLVERS.resolveMetadata(true, resolvedFields, options, ss);
        KvMetadata valueMetadata = METADATA_RESOLVERS.resolveMetadata(false, resolvedFields, options, ss);
        List<TableField> fields = concat(keyMetadata.getFields().stream(), valueMetadata.getFields().stream())
                .collect(toList());

        MapService service = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext context = service.getMapServiceContext();
        MapContainer container = context.getMapContainer(externalName);

        long estimatedRowCount = estimatePartitionedMapRowCount(nodeEngine, context, externalName);
        boolean hd = container != null && container.getMapConfig().getInMemoryFormat() == InMemoryFormat.NATIVE;

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
                Collections.emptyList(),
                hd
        );
    }

    @Nonnull
    @Override
    public Vertex fullScanReader(
            @Nonnull DAG dag,
            @Nonnull Table table0,
            @Nullable Expression<Boolean> filter,
            @Nonnull List<Expression<?>> projection
    ) {
        PartitionedMapTable table = (PartitionedMapTable) table0;
        MapScanMetadata mapScanMetadata = new MapScanMetadata(
                table.getMapName(),
                table.getKeyDescriptor(),
                table.getValueDescriptor(),
                asList(table.paths()),
                asList(table.types()),
                projection,
                filter
        );

        return dag.newUniqueVertex(toString(table), OnHeapMapScanP.onHeapMapScanP(mapScanMetadata));
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

        String name = table.getMapName();
        QueryPath[] paths = table.paths();
        QueryDataType[] types = table.types();
        QueryTargetDescriptor keyDescriptor = table.getKeyDescriptor();
        QueryTargetDescriptor valueDescriptor = table.getValueDescriptor();

        KvRowProjector.Supplier rightRowProjectorSupplier =
                KvRowProjector.supplier(paths, types, keyDescriptor, valueDescriptor, predicate, projections);

        return IMapJoiner.join(dag, name, toString(table), joinInfo, rightRowProjectorSupplier);
    }

    @Override
    public boolean requiresSink() {
        return true;
    }

    @Nonnull
    @Override
    public Vertex sink(
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
                        (UpsertTargetDescriptor) table.getValueJetMetadata()
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
            @Nonnull List<String> updatedFields,
            @Nonnull List<Expression<?>> updates
    ) {
        PartitionedMapTable table = (PartitionedMapTable) table0;

        QueryPath[] paths = table.paths();
        QueryDataType[] types = table.types();

        List<TableField> fields = table.getFields();
        int keyIndex = -1;
        List<Expression<?>> projections = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            MapTableField field = ((MapTableField) fields.get(i));

            if (field.getPath().isKey() && updatedFields.contains(field.getName())) {
                throw QueryException.error("Cannot update key");
            }

            if (field.getPath().equals(KEY_PATH)) {
                assert keyIndex == -1;
                keyIndex = i;
            }

            projections.add(ColumnExpression.create(i, field.getType()));
        }

        KvRowProjector.Supplier rowProjectorSupplier = KvRowProjector.supplier(
                paths,
                types,
                table.getKeyDescriptor(),
                table.getValueDescriptor(),
                null,
                projections
        );
        KvProjector.Supplier projectorSupplier = KvProjector.supplier(
                paths,
                types,
                (UpsertTargetDescriptor) table.getKeyJetMetadata(),
                (UpsertTargetDescriptor) table.getValueJetMetadata()
        );

        return dag.newUniqueVertex(
                "Update(" + toString(table) + ")",
                new UpdateProcessorSupplier(table.getMapName(), keyIndex, rowProjectorSupplier, updates, projectorSupplier)
        );
    }

    @Nonnull
    @Override
    public Vertex delete(@Nonnull DAG dag, @Nonnull Table table0) {
        PartitionedMapTable table = (PartitionedMapTable) table0;

        return dag.newUniqueVertex(
                toString(table),
                // TODO do a simpler, specialized deleting-only processor
                updateMapP(table.getMapName(), (FunctionEx<Object[], Object>) row -> {
                    assert row.length == 1;
                    return row[0];
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
