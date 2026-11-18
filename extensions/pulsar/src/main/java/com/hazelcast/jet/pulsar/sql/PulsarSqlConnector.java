/*
 * Copyright 2026 Hazelcast Inc.
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
package com.hazelcast.jet.pulsar.sql;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.pulsar.impl.PulsarSinkConfig;
import com.hazelcast.jet.pulsar.impl.PulsarSourceConfig;
import com.hazelcast.jet.sql.impl.connector.HazelcastRexNode;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.keyvalue.JavaClassNameResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataAvroResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataJavaResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataJsonResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataNullResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolvers;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Locale.ROOT;

@SuppressWarnings("rawtypes")
public class PulsarSqlConnector implements SqlConnector {

    private static final KvMetadataResolvers METADATA_RESOLVERS = new KvMetadataResolvers(
            new KvMetadataResolver[]{
                KvMetadataNullResolver.INSTANCE,
                KvMetadataJavaResolver.INSTANCE
            },
            new KvMetadataResolver[]{
                KvMetadataJavaResolver.INSTANCE,
                KvMetadataJsonResolver.INSTANCE,
                KvMetadataAvroResolver.INSTANCE
            }
    );

    @Override
    public String typeName() {
        return "Pulsar";
    }

    @Override
    public @Nonnull String defaultObjectType() {
        return "Topic";
    }

    @Override
    public @Nonnull List<MappingField> resolveAndValidateFields(@Nonnull NodeEngine nodeEngine,
                                                                @Nonnull SqlConnector.SqlExternalResource externalResource,
                                                                @Nonnull List<MappingField> userFields) {

        validateKeyType(userFields, externalResource.options());

        return METADATA_RESOLVERS.resolveAndValidateFields(userFields, externalResource.options(), nodeEngine);
    }

    private void validateKeyType(List<MappingField> userFields, Map<String, String> options) {
        var keyFieldOpt = userFields.stream().filter(PulsarSqlConnector::isKeyField).toList();
        if (keyFieldOpt.isEmpty()) {
            return;
        }
        if (keyFieldOpt.size() > 1) {
            throw QueryException.error("More than one field specified as part of " + QueryPath.KEY + " field");
        }
        var keyField = keyFieldOpt.get(0);
        QueryDataType type = keyField.type();
        String keyFormat = options.get(OPTION_KEY_FORMAT);
        if (keyFormat != null) {
            keyFormat = keyFormat.toLowerCase(ROOT);
        }
        List<String> allowedStringTypes = JavaClassNameResolver.resolveByClassName(String.class);
        if (!allowedStringTypes.contains(keyFormat)) {
            throw QueryException.error(OPTION_KEY_FORMAT + " option must be one of: `java` or " + allowedStringTypes);
        }
        if ("java".equals(keyFormat)) {
            String lowerCase = options.get(OPTION_KEY_CLASS).toLowerCase(ROOT);
            if (!lowerCase.equals("string") && !lowerCase.equals("java.lang.string")) {
                throw QueryException.error("If key is of format `java`, it must have "
                                               + OPTION_KEY_CLASS + " set to java.lang.String");
            }
        }

        if (type.getTypeFamily() != QueryDataTypeFamily.VARCHAR) {
            throw QueryException.error(QueryPath.KEY + " field must be of one of types: " + allowedStringTypes);
        }
    }

    private static boolean isKeyField(MappingField field) {
        String nameLowerCase = field.name().toLowerCase(ROOT);
        String externalNameLowerCase = field.externalName() == null ? "" : field.externalName().toLowerCase(ROOT);
        return nameLowerCase.equals(QueryPath.KEY) || externalNameLowerCase.equals(QueryPath.KEY)
            || nameLowerCase.startsWith(QueryPath.KEY_PREFIX)
            || externalNameLowerCase.startsWith(QueryPath.KEY_PREFIX);
    }

    @Override
    public @Nonnull Table createTable(@Nonnull NodeEngine nodeEngine,
                                      @Nonnull String schemaName,
                                      @Nonnull String mappingName,
                                      @Nonnull SqlConnector.SqlExternalResource externalResource,
                                      @Nonnull List<MappingField> resolvedFields) {
        final Map<String, String> options = externalResource.options();
        KvMetadata keyMetadata = METADATA_RESOLVERS.resolveMetadata(true, resolvedFields, options, null);
        KvMetadata valueMetadata = METADATA_RESOLVERS.resolveMetadata(false, resolvedFields, options, null);
        List<TableField> fields = Stream.concat(keyMetadata.getFields().stream(), valueMetadata.getFields().stream()).toList();

        return new PulsarTable(
                this,
                schemaName,
                mappingName,
                fields,
                new ConstantTableStatistics(0),
                externalResource.externalName()[0],
                externalResource.dataConnection(),
                options,
                keyMetadata.getQueryTargetDescriptor(),
                keyMetadata.getUpsertTargetDescriptor(),
                valueMetadata.getQueryTargetDescriptor(),
                valueMetadata.getUpsertTargetDescriptor(),
                externalResource.objectType()
        );
    }

    @Nonnull
    @Override
    public Vertex fullScanReader(
        @Nonnull DagBuildContext context,
        @Nullable HazelcastRexNode predicate,
        @Nonnull List<HazelcastRexNode> projection,
        @Nullable List<Map<String, Expression<?>>> partitionPruningCandidates,
        @Nullable FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider
    ) {
        PulsarTable table = context.getTable();
        PulsarSourceConfig conf = table.buildConfigForSql(PulsarSourceConfig::new);

        return context.getDag().newUniqueVertex(
            table.toString(),
            ProcessorMetaSupplier.of(
                        table.preferredLocalParallelism(),
                        new RowProjectorProcessorSupplier(
                            conf,
                            eventTimePolicyProvider,
                            table.paths(),
                            table.types(),
                            table.keyQueryDescriptor(),
                            table.valueQueryDescriptor(),
                            context.convertFilter(predicate),
                            context.convertProjection(projection)
                        )
                )
        );
    }
    @Nonnull
    private static Vertex writeProcessor(@Nonnull DagBuildContext context) {
        PulsarTable table = context.getTable();

        PulsarSinkConfig conf = table.buildConfigForSql(PulsarSinkConfig::new);
        return context.getDag().newUniqueVertex(table.toString(),
                                                new WriteRowProcessorSupplier(conf,
                                                                              table.paths(),
                                                                              table.types(),
                                                                              table.keyUpsertDescriptor(),
                                                                              table.valueUpsertDescriptor()));
    }

    @Nonnull
    @Override
    public VertexWithInputConfig insertProcessor(@Nonnull DagBuildContext context) {
        return new VertexWithInputConfig(writeProcessor(context));
    }

    @Nonnull
    @Override
    public Vertex sinkProcessor(@Nonnull DagBuildContext context) {
        return writeProcessor(context);
    }
}
