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

package com.hazelcast.jet.sql.impl.connector.file;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.connector.HazelcastRexNode;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.SqlProcessors;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.security.Permission;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static com.hazelcast.security.permission.ConnectorPermission.file;
import static java.util.Collections.singletonList;

public class FileSqlConnector implements SqlConnector {

    public static final String TYPE_NAME = "File";

    public static final String OPTION_PATH = "path";
    public static final String OPTION_GLOB = "glob";
    public static final String OPTION_SHARED_FILE_SYSTEM = "sharedFileSystem";
    public static final String OPTION_IGNORE_FILE_NOT_FOUND = "ignoreFileNotFound";
    public static final String OPTION_OPTIONS = "options";

    static final FileSqlConnector INSTANCE = new FileSqlConnector();

    private static final MetadataResolvers METADATA_RESOLVERS = new MetadataResolvers(
            CsvMetadataResolver.INSTANCE,
            JsonMetadataResolver.INSTANCE,
            AvroMetadataResolver.INSTANCE,
            ParquetMetadataResolver.INSTANCE
    );

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Nonnull
    @Override
    public String defaultObjectType() {
        return "File";
    }

    @Nonnull
    @Override
    public List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> userFields) {
        return resolveAndValidateFields(externalResource.options(), userFields);
    }

    @Nonnull
    static List<MappingField> resolveAndValidateFields(
            @Nonnull Map<String, ?> options,
            @Nonnull List<MappingField> userFields
    ) {
        return METADATA_RESOLVERS.resolveAndValidateFields(userFields, options);
    }

    @Nonnull
    @Override
    public List<Permission> permissionsForResolve(SqlExternalResource resource, NodeEngine nodeEngine) {
        return singletonList(file(resource.options().get(OPTION_PATH), ACTION_READ));
    }

    @Nonnull
    @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String mappingName,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> resolvedFields) {
        Metadata metadata = METADATA_RESOLVERS.resolveMetadata(resolvedFields, externalResource.options());

        return new FileTable.SpecificFileTable(
                INSTANCE,
                schemaName,
                mappingName,
                metadata.fields(),
                metadata.processorMetaSupplier(),
                metadata.queryTargetSupplier(),
                externalResource.objectType()
        );
    }

    @Nonnull
    @SuppressWarnings("SameParameterValue")
    static Table createTable(
            @Nonnull String schemaName,
            @Nonnull String name,
            @Nonnull Map<String, ?> options,
            @Nonnull List<MappingField> resolvedFields
    ) {
        Metadata metadata = METADATA_RESOLVERS.resolveMetadata(resolvedFields, options);

        return new FileTable.DynamicFileTable(
                INSTANCE,
                schemaName,
                name,
                metadata.fields(),
                metadata.processorMetaSupplier(),
                metadata.queryTargetSupplier()
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
        if (eventTimePolicyProvider != null) {
            throw QueryException.error("Ordering functions are not supported on top of " + TYPE_NAME + " mappings");
        }

        FileTable table = context.getTable();

        Vertex vStart = context.getDag().newUniqueVertex(table.toString(), table.processorMetaSupplier());

        Vertex vEnd = context.getDag().newUniqueVertex(
                "Project(" + table + ")",
                SqlProcessors.rowProjector(
                        table.paths(),
                        table.types(),
                        table.queryTargetSupplier(),
                        context.convertFilter(predicate),
                        context.convertProjection(projection)
                )
        );

        context.getDag().edge(between(vStart, vEnd).isolated());
        return vEnd;
    }

    @Override
    public boolean supportsExpression(@Nonnull HazelcastRexNode expression) {
        return true;
    }

    @Override
    public Set<String> nonSensitiveConnectorOptions() {
        Set<String> set = SqlConnector.super.nonSensitiveConnectorOptions();
        // Note: OPTION_PATH and OPTION_GLOB are considered sensitive and won't be returned.
        set.add(OPTION_SHARED_FILE_SYSTEM);
        set.add(OPTION_IGNORE_FILE_NOT_FOUND);
        return set;
    }
}
