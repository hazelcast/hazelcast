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

package com.hazelcast.jet.sql.impl.connector.generator;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.pipeline.transform.BatchSourceTransform;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.jet.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

class SeriesSqlConnector implements SqlConnector {

    static final SeriesSqlConnector INSTANCE = new SeriesSqlConnector();

    private static final String TYPE_NAME = "Series";
    private static final List<TableField> FIELDS = singletonList(new TableField("v", QueryDataType.INT, false));

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
        throw new UnsupportedOperationException("Resolving fields not supported for " + typeName());
    }

    @Nonnull
    @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String name,
            @Nonnull String externalName,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> resolvedFields
    ) {
        throw new UnsupportedOperationException("Creating table not supported for " + typeName());
    }

    @Nonnull
    @SuppressWarnings("SameParameterValue")
    static SeriesTable createTable(String schemaName, String name, List<Expression<?>> argumentExpressions) {
        return new SeriesTable(INSTANCE, FIELDS, schemaName, name, argumentExpressions);
    }

    @Nonnull
    @Override
    public Vertex fullScanReader(
            @Nonnull DAG dag,
            @Nonnull Table table0,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projections
    ) {
        SeriesTable table = (SeriesTable) table0;

        BatchSource<Object[]> source = table.items(predicate, projections);
        ProcessorMetaSupplier pms = ((BatchSourceTransform<Object[]>) source).metaSupplier;
        return dag.newUniqueVertex(table.toString(), pms);
    }
}
