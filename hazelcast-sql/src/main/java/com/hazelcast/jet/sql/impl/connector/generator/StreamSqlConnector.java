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

package com.hazelcast.jet.sql.impl.connector.generator;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.sql.impl.connector.HazelcastRexNode;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.insertWatermarksP;
import static java.util.Collections.singletonList;

public class StreamSqlConnector implements SqlConnector {

    static final StreamSqlConnector INSTANCE = new StreamSqlConnector();

    private static final String TYPE_NAME = "Stream";
    private static final List<TableField> FIELDS = singletonList(new TableField("v", QueryDataType.BIGINT, false));

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Nonnull
    @Override
    public String defaultObjectType() {
        return "Stream";
    }

    @Nonnull
    @Override
    public List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> userFields) {
        throw new UnsupportedOperationException("Resolving fields not supported for " + typeName());
    }

    @Nonnull
    @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String mappingName,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> resolvedFields) {
        throw new UnsupportedOperationException("Creating table not supported for " + typeName());
    }

    @Nonnull
    @SuppressWarnings("SameParameterValue")
    public static StreamTable createTable(String schemaName, String name, List<Expression<?>> argumentExpressions) {
        return new StreamTable(INSTANCE, FIELDS, schemaName, name, argumentExpressions);
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
        StreamTable table = context.getTable();
        StreamSourceTransform<JetSqlRow> source = (StreamSourceTransform<JetSqlRow>) table.items(
                context.convertFilter(predicate), context.convertProjection(projection));
        ProcessorMetaSupplier pms = source.metaSupplierFn.apply(EventTimePolicy.noEventTime());
        Vertex vertex = context.getDag().newUniqueVertex(table.toString(), pms);
        // add watermark generator, if requested
        if (eventTimePolicyProvider != null) {
            Vertex addWm = context.getDag().newUniqueVertex("addWm",
                    insertWatermarksP(ctx -> eventTimePolicyProvider.apply(ExpressionEvalContext.from(ctx))));
            context.getDag().edge(between(vertex, addWm).isolated());
            vertex = addWm;
        }
        return vertex;
    }

    @Override
    public boolean supportsExpression(@Nonnull HazelcastRexNode expression) {
        return true;
    }
}
