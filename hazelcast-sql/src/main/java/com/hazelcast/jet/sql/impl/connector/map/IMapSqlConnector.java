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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningAttributeConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.jet.core.DefaultPartitionStrategy;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.sql.impl.CalciteSqlOptimizer;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.HazelcastRexNode;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataJavaResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolvers;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvProcessors;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvProjector;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.opt.physical.DagBuildContextImpl;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.partition.strategy.AttributePartitioningStrategy;
import com.hazelcast.partition.strategy.DefaultPartitioningStrategy;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUuidString;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.updateMapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.JobRepository.INTERNAL_JET_OBJECTS_PREFIX;
import static com.hazelcast.jet.sql.impl.connector.map.MapIndexScanP.readMapIndexSupplier;
import static com.hazelcast.jet.sql.impl.connector.map.RowProjectorProcessorSupplier.rowProjector;
import static com.hazelcast.jet.sql.impl.connector.map.SpecificPartitionsImapReaderPms.mapReader;
import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.sql.impl.QueryUtils.getMapContainer;
import static com.hazelcast.sql.impl.QueryUtils.quoteCompoundIdentifier;
import static com.hazelcast.sql.impl.schema.map.MapTableUtils.estimatePartitionedMapRowCount;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

public class IMapSqlConnector implements SqlConnector {

    public static final IMapSqlConnector INSTANCE = new IMapSqlConnector();

    public static final String TYPE_NAME = "IMap";
    public static final String OBJECT_TYPE_IMAP = "IMap";
    public static final List<String> PRIMARY_KEY_LIST = singletonList(QueryPath.KEY);

    private static final KvMetadataResolvers METADATA_RESOLVERS_WITH_COMPACT = new KvMetadataResolvers(
            KvMetadataJavaResolver.INSTANCE,
            MetadataPortableResolver.INSTANCE,
            MetadataCompactResolver.INSTANCE,
            MetadataJsonResolver.INSTANCE
    );

    private static final Tuple2<PartitioningStrategy<?>, List<List<Expression<?>>>> NO_PARTITION_PRUNING = tuple2(null, null);

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Nonnull
    @Override
    public String defaultObjectType() {
        return OBJECT_TYPE_IMAP;
    }

    @Nonnull
    @Override
    public List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> userFields) {
        checkImapName(externalResource.externalName());
        return METADATA_RESOLVERS_WITH_COMPACT.resolveAndValidateFields(userFields, externalResource.options(), nodeEngine);
    }

    @Nonnull
    @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String mappingName,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> resolvedFields) {
        checkImapName(externalResource.externalName());

        InternalSerializationService ss = (InternalSerializationService) nodeEngine.getSerializationService();

        KvMetadata keyMetadata = METADATA_RESOLVERS_WITH_COMPACT.resolveMetadata(
                true,
                resolvedFields,
                externalResource.options(), ss
        );
        KvMetadata valueMetadata = METADATA_RESOLVERS_WITH_COMPACT.resolveMetadata(
                false,
                resolvedFields,
                externalResource.options(),
                ss
        );
        List<TableField> fields = concat(keyMetadata.getFields().stream(), valueMetadata.getFields().stream())
                .collect(toList());

        MapService service = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext context = service.getMapServiceContext();
        String mapName = externalResource.externalName()[0];
        MapContainer container = context.getExistingMapContainer(mapName);

        long estimatedRowCount = estimatePartitionedMapRowCount(nodeEngine, context, mapName);
        boolean hd = container != null && container.getMapConfig().getInMemoryFormat() == InMemoryFormat.NATIVE;
        List<MapTableIndex> indexes = container != null
                ? MapTableUtils.getPartitionedMapIndexes(container, fields)
                : emptyList();

        final List<String> partitioningAttributes = nodeEngine.getConfig()
                .getMapConfig(mapName)
                .getPartitioningAttributeConfigs().stream()
                .map(PartitioningAttributeConfig::getAttributeName)
                .collect(toList());

        return new PartitionedMapTable(
                schemaName,
                mappingName,
                mapName,
                fields,
                new ConstantTableStatistics(estimatedRowCount),
                keyMetadata.getQueryTargetDescriptor(),
                valueMetadata.getQueryTargetDescriptor(),
                keyMetadata.getUpsertTargetDescriptor(),
                valueMetadata.getUpsertTargetDescriptor(),
                indexes,
                hd,
                partitioningAttributes,
                supportsPartitionPruning(nodeEngine, mapName));
    }

    private boolean supportsPartitionPruning(final NodeEngine nodeEngine, final String mapName) {
        final MapConfig mapConfig = nodeEngine.getConfig().getMapConfig(mapName);
        if (!mapConfig.getPartitioningAttributeConfigs().isEmpty()) {
            return true;
        }

        final PartitioningStrategyConfig strategyConfig = mapConfig.getPartitioningStrategyConfig();
        if (strategyConfig == null) {
            return true;
        }

        if (StringUtil.isNullOrEmpty(strategyConfig.getPartitioningStrategyClass())
                && strategyConfig.getPartitioningStrategy() == null) {
            return true;
        }

        if (strategyConfig.getPartitioningStrategy() != null) {
            return strategyConfig.getPartitioningStrategy() instanceof DefaultPartitionStrategy;
        }

        if (!StringUtil.isNullOrEmpty(strategyConfig.getPartitioningStrategyClass())) {
            return strategyConfig.getPartitioningStrategyClass().equals(DefaultPartitionStrategy.class.getName());
        }

        return false;
    }

    private static void checkImapName(@Nonnull String[] externalName) {
        if (externalName.length > 1) {
            throw QueryException.error("Invalid external name " + quoteCompoundIdentifier(externalName)
                    + ", external name for IMap is allowed to have only a single component referencing the map name");
        }
        String mapName = externalName[0];
        if (mapName.startsWith(INTERNAL_JET_OBJECTS_PREFIX) || mapName.equals(JetServiceBackend.SQL_CATALOG_MAP_NAME)) {
            throw QueryException.error("Mapping of internal IMaps is not allowed");
        }
    }

    @Nonnull
    @Override
    public Vertex fullScanReader(
            @Nonnull DagBuildContext context,
            @Nullable HazelcastRexNode filter,
            @Nonnull List<HazelcastRexNode> projection,
            @Nullable List<Map<String, Expression<?>>> partitionPruningCandidates,
            @Nullable FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider
    ) {
        if (eventTimePolicyProvider != null) {
            throw QueryException.error("Ordering functions are not supported on top of " + TYPE_NAME + " mappings");
        }

        PartitionedMapTable table = context.getTable();

        if (partitionPruningCandidates == null && !table.partitioningAttributes().isEmpty()) {
            // We have an IMap but the query cannot use member pruning.
            // Maybe we still can use scan partition pruning.
            // TODO: this would be better done if we could reuse results of the analysis
            //  done for member pruning, eg. if it was available in each RelNode

            // We need some low-level data which are not passed to SqlConnector, this code should be refactored.
            DagBuildContextImpl contextImpl = (DagBuildContextImpl) context;
            var relPrunability = CalciteSqlOptimizer.partitionStrategyCandidates(contextImpl.getRel(),
                    contextImpl.getParameterMetadata(),
                    // expect only single map in the rel
                    Map.of(table.getSqlName(), table));
            partitionPruningCandidates = relPrunability.get(table.getSqlName());
        }

        Tuple2<PartitioningStrategy<?>, List<List<Expression<?>>>> requiredPartitionsExprs =
                computeRequiredPartitionsToScan(context.getNodeEngine(), partitionPruningCandidates, table.getMapName());
        Vertex vStart = context.getDag().newUniqueVertex(
                toString(table),
                requiredPartitionsExprs.f1() != null
                        // pruned
                    ? mapReader(table.getMapName(), requiredPartitionsExprs.f0(), requiredPartitionsExprs.f1())
                        // not pruned
                    : readMapP(table.getMapName())
        );

        Vertex vEnd = context.getDag().newUniqueVertex(
                "Project(" + toString(table) + ")",
                rowProjector(
                        table.paths(),
                        table.types(),
                        table.getKeyDescriptor(),
                        table.getValueDescriptor(),
                        context.convertFilter(filter),
                        context.convertProjection(projection)
                )
        );

        context.getDag().edge(Edge.from(vStart).to(vEnd).isolated());
        return vEnd;
    }

    @Nonnull
    @SuppressWarnings("checkstyle:ParameterNumber")
    public Vertex indexScanReader(
            @Nonnull DagBuildContext context,
            @Nonnull Address localMemberAddress,
            @Nonnull MapTableIndex tableIndex,
            @Nullable HazelcastRexNode remainingFilter,
            @Nonnull List<HazelcastRexNode> projection,
            @Nullable IndexFilter indexFilter,
            @Nullable ComparatorEx<JetSqlRow> comparator,
            boolean descending,
            boolean requiresSort) {
        PartitionedMapTable table = context.getTable();
        MapIndexScanMetadata indexScanMetadata = new MapIndexScanMetadata(
                table.getMapName(),
                tableIndex.getName(),
                table.getKeyDescriptor(),
                table.getValueDescriptor(),
                asList(table.paths()),
                asList(table.types()),
                indexFilter,
                context.convertProjection(projection),
                context.convertFilter(remainingFilter),
                comparator,
                descending
        );

        Vertex scanner = context.getDag().newUniqueVertex(
                "Index(" + toString(table) + ")",
                readMapIndexSupplier(indexScanMetadata)
        );
        // LP must be 1 - one partitioned index contains all local partitions, if there are 2 local processors,
        // the index will be scanned twice and each time half of the partitions will be thrown out.
        scanner.localParallelism(1);

        if (tableIndex.getType() == IndexType.SORTED && requiresSort) {
            Vertex sorter = context.getDag().newUniqueVertex(
                    "SortCombine",
                    ProcessorMetaSupplier.forceTotalParallelismOne(
                            ProcessorSupplier.of(mapP(FunctionEx.identity())),
                            localMemberAddress
                    )
            );

            assert comparator != null;
            context.getDag().edge(between(scanner, sorter)
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
            @Nonnull DagBuildContext context,
            @Nullable HazelcastRexNode predicate,
            @Nonnull List<HazelcastRexNode> projections,
            @Nonnull JetJoinInfo joinInfo
    ) {
        PartitionedMapTable table = context.getTable();

        KvRowProjector.Supplier rightRowProjectorSupplier = KvRowProjector.supplier(
                table.paths(),
                table.types(),
                table.getKeyDescriptor(),
                table.getValueDescriptor(),
                context.convertFilter(predicate),
                context.convertProjection(projections)
        );

        return Joiner.join(context.getDag(), table.getMapName(), toString(table), joinInfo, rightRowProjectorSupplier);
    }

    @Nonnull
    @Override
    public VertexWithInputConfig insertProcessor(@Nonnull DagBuildContext context) {
        PartitionedMapTable table = context.getTable();

        Vertex vertex = context.getDag().newUniqueVertex(
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
    public Vertex sinkProcessor(@Nonnull DagBuildContext context) {
        PartitionedMapTable table = context.getTable();

        Vertex vStart = context.getDag().newUniqueVertex(
                "Project(" + toString(table) + ")",
                KvProcessors.entryProjector(
                        table.paths(),
                        table.types(),
                        (UpsertTargetDescriptor) table.getKeyJetMetadata(),
                        (UpsertTargetDescriptor) table.getValueJetMetadata(),
                        true
                )
        );

        Vertex vEnd = context.getDag().newUniqueVertex(
                toString(table),
                writeMapP(table.getMapName())
        );

        context.getDag().edge(between(vStart, vEnd));
        return vStart;
    }

    @Nonnull
    @Override
    public Vertex updateProcessor(
            @Nonnull DagBuildContext context,
            @Nonnull List<String> fieldNames,
            @Nonnull List<HazelcastRexNode> expressions,
            @Nullable HazelcastRexNode predicate,
            boolean hasInput
    ) {
        assert predicate == null;
        assert hasInput;
        PartitionedMapTable table = context.getTable();

        return context.getDag().newUniqueVertex(
                "Update(" + toString(table) + ")",
                new UpdateProcessorSupplier(
                        table.getMapName(),
                        UpdatingEntryProcessor.supplier(table, fieldNames, context.convertProjection(expressions))
                )
        );
    }

    @Nonnull
    @Override
    public Vertex deleteProcessor(@Nonnull DagBuildContext context, @Nullable HazelcastRexNode predicate, boolean hasInput) {
        assert predicate == null;
        assert hasInput;
        PartitionedMapTable table = context.getTable();

        return context.getDag().newUniqueVertex(
                toString(table),
                // TODO do a simpler, specialized deleting-only processor
                updateMapP(table.getMapName(), (FunctionEx<JetSqlRow, Object>) row -> {
                    assert row.getFieldCount() == 1;
                    return row.get(0);
                }, (v, t) -> null));
    }

    @Override
    public boolean dmlSupportsPredicates() {
        return false;
    }

    @Override
    public boolean supportsExpression(@Nonnull HazelcastRexNode expression) {
        return true;
    }

    @Nonnull
    @Override
    public List<String> getPrimaryKey(Table table0) {
        return PRIMARY_KEY_LIST;
    }

    @Nullable
    private Tuple2<PartitioningStrategy<?>, List<List<Expression<?>>>> computeRequiredPartitionsToScan(
            NodeEngine nodeEngine,
            List<Map<String, Expression<?>>> candidates,
            String mapName
    ) {
        if (candidates == null) {
            return NO_PARTITION_PRUNING;
        }

        List<List<Expression<?>>> partitionsExpressions = new ArrayList<>();

        final var container = getMapContainer(nodeEngine.getHazelcastInstance().getMap(mapName));
        final PartitioningStrategy<?> strategy = container.getPartitioningStrategy();

        // We only support Default and Attribute strategies, even if one of the maps uses non-Default/Attribute
        // strategy, we should abort the process and clear list of already populated partitions so that partition
        // pruning doesn't get activated at all for this query.
        if (strategy != null
                && !(strategy instanceof DefaultPartitioningStrategy)
                && !(strategy instanceof AttributePartitioningStrategy)) {
            return NO_PARTITION_PRUNING;
        }

        // ordering of attributes matters for partitioning (1,2) produces different partition than (2,1).
        final List<String> orderedKeyAttributes = new ArrayList<>();
        if (strategy instanceof AttributePartitioningStrategy) {
            final var attributeStrategy = (AttributePartitioningStrategy) strategy;
            orderedKeyAttributes.addAll(asList(attributeStrategy.getPartitioningAttributes()));
        } else {
            orderedKeyAttributes.add(KEY_ATTRIBUTE_NAME.value());
        }

        for (final Map<String, Expression<?>> perMapCandidate : candidates) {
            List<Expression<?>> expressions = new ArrayList<>();
            for (final String attribute : orderedKeyAttributes) {
                if (!perMapCandidate.containsKey(attribute)) {
                    // Shouldn't happen, defensive check in case Opt logic breaks and produces variants
                    // that do not contain all the required partitioning attributes.
                    throw new HazelcastException("Partition Pruning candidate"
                            + " does not contain mandatory attribute: " + attribute);
                }
                expressions.add(perMapCandidate.get(attribute));
            }
            partitionsExpressions.add(expressions);
        }
        return tuple2(strategy, partitionsExpressions);
    }

    private static String toString(PartitionedMapTable table) {
        return TYPE_NAME + "[" + table.getSchemaName() + "." + table.getSqlName() + "]";
    }
}
