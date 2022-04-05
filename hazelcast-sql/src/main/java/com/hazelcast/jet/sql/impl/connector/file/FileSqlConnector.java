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

package com.hazelcast.jet.sql.impl.connector.file;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.SqlProcessors;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.core.Edge.between;

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
        return resolveAndValidateFields(options, userFields);
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
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String mappingName,
            @Nonnull String externalName,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> resolvedFields
    ) {
        Metadata metadata = METADATA_RESOLVERS.resolveMetadata(resolvedFields, options);

        return new FileTable.SpecificFileTable(
                INSTANCE,
                schemaName,
                mappingName,
                metadata.fields(),
                metadata.processorMetaSupplier(),
                metadata.queryTargetSupplier()
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
            @Nonnull DAG dag,
            @Nonnull Table table0,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projections,
            @Nullable FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider
    ) {
        if (eventTimePolicyProvider != null) {
            throw QueryException.error("Ordering functions are not supported on top of " + TYPE_NAME + " mappings");
        }

        FileTable table = (FileTable) table0;

        Vertex vStart = dag.newUniqueVertex(table.toString(), table.processorMetaSupplier());

        Vertex vEnd = dag.newUniqueVertex(
                "Project(" + table + ")",
                SqlProcessors.rowProjector(
                        table.paths(),
                        table.types(),
                        table.queryTargetSupplier(),
                        predicate,
                        projections
                )
        );

        dag.edge(between(vStart, vEnd).isolated());
        return vEnd;
    }
}
