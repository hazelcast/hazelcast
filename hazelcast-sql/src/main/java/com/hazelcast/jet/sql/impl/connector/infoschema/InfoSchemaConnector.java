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

package com.hazelcast.jet.sql.impl.connector.infoschema;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.connector.HazelcastRexNode;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;

/**
 * A connector for tables in the {@code information_schema}.
 */
final class InfoSchemaConnector implements SqlConnector {

    public static final InfoSchemaConnector INSTANCE = new InfoSchemaConnector();

    private static final String TYPE_NAME = "InformationSchema";

    private InfoSchemaConnector() {
    }

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Nonnull
    @Override
    public String defaultObjectType() {
        return "InfoSchema";
    }

    @Nonnull
    @Override
    public List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> userFields) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String mappingName,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> resolvedFields) {
        throw new UnsupportedOperationException();
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

        InfoSchemaTable table = context.getTable();
        List<Object[]> rows = table.rows();
        Expression<Boolean> convertedPredicate = context.convertFilter(predicate);
        List<Expression<?>> convertedProjection = context.convertProjection(projection);
        return context.getDag().newUniqueVertex(
                table.toString(),
                forceTotalParallelismOne(ProcessorSupplier.of(() ->
                        new StaticSourceP(convertedPredicate, convertedProjection, rows)))
        );
    }

    @Override
    public boolean supportsExpression(@Nonnull HazelcastRexNode expression) {
        return true;
    }

    private static final class StaticSourceP extends AbstractProcessor {

        private final Expression<Boolean> predicate;
        private final List<Expression<?>> projection;
        private final List<Object[]> rows;

        private Traverser<JetSqlRow> traverser;

        private StaticSourceP(Expression<Boolean> predicate, List<Expression<?>> projection, List<Object[]> rows) {
            this.predicate = predicate;
            this.projection = projection;
            this.rows = rows;
        }

        @Override
        protected void init(@Nonnull Context context) {
            ExpressionEvalContext evalContext = ExpressionEvalContext.from(context);
            List<JetSqlRow> processedRows = ExpressionUtil.evaluate(predicate, projection,
                    rows.stream().map(row -> new JetSqlRow(evalContext.getSerializationService(), row)), evalContext);
            traverser = Traversers.traverseIterable(processedRows);
        }

        @Override
        public boolean isCooperative() {
            return (predicate == null || predicate.isCooperative()) && projection.stream().allMatch(Expression::isCooperative);
        }

        @Override
        public boolean complete() {
            return emitFromTraverser(traverser);
        }

        @Override
        public boolean closeIsCooperative() {
            return true;
        }
    }
}
