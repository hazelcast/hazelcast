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
package com.hazelcast.jet.sql.impl.connector.mongodb;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.SqlProcessors;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import org.apache.calcite.rex.RexNode;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.mongodb.Mappers.defaultCodecRegistry;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

/**
 * Batch-query MongoDB SQL Connector.
 * TODO: add more
 */
public class MongoBatchSqlConnector implements SqlConnector {

    private final FieldResolver fieldResolver = new FieldResolver();

    @Override
    public String typeName() {
        return "MongoDB";
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
        return fieldResolver.resolveFields(options, userFields);
    }

    @Nonnull
    @Override
    public Table createTable(@Nonnull NodeEngine nodeEngine, @Nonnull String schemaName, @Nonnull String mappingName,
                             @Nonnull String externalName, @Nonnull Map<String, String> options,
                             @Nonnull List<MappingField> resolvedFields) {
        String databaseName = options.get(Options.DATABASE_NAME_OPTION);
        String collectionName = options.get(Options.COLLECTION_NAME_OPTION);
        ConstantTableStatistics stats = new ConstantTableStatistics(0);

        List<TableField> fields = new ArrayList<>(resolvedFields.size());
        for (MappingField resolvedField : resolvedFields) {
            String fieldExternalName = firstNonNull(resolvedField.externalName(), resolvedField.name());

            fields.add(new MongoTableField(
                    resolvedField.name(),
                    resolvedField.type(),
                    fieldExternalName,
                    resolvedField.isPrimaryKey()
            ));
        }
        return new MongoTable(schemaName, mappingName, databaseName, collectionName, options, this,
                fields, stats);
    }

    @Override
    @Nonnull
    public Vertex fullScanReader(
            @Nonnull DagBuildContext context,
            @Nullable RexNode predicate,
            @Nonnull List<RexNode> projection,
            @Nullable FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider) {
        MongoTable table = context.getTable();

        ExpressionToMongoVisitor visitor = new ExpressionToMongoVisitor(table, null, true);

        TranslationResult<String> filter = translateFilter(predicate, context, visitor);
        TranslationResult<List<String>> projections = translateProjections(projection, context, visitor);

        DAG dag = context.getDag();

        // if not all filters are pushed down, then we cannot push down projection
        // because later filters may use those fields
        // We could do it smarter and check field usage in the filters, but it's something TODO
        List<String> projectionList = filter.allProceeded ? projections.result : emptyList();
        Vertex sourceVertex = dag.newUniqueVertex(
                "Select (" + table.getSqlName() + ")",
                new SelectProcessorSupplier(table, filter.result, projectionList)
        );

        if (!filter.allProceeded || !projections.allProceeded) {
            List<Expression<?>> projectionExpr = context.convertProjection(projection);
            Expression<Boolean> filterExpr = context.convertFilter(predicate);
            Vertex vEnd = dag.newUniqueVertex(
                    "ProjectAndFilter(" + table + ")",
                    SqlProcessors.rowProjector(
                            table.paths(),
                            table.types(),
                            table.queryTargetSupplier(),
                            filterExpr,
                            projectionExpr
                    )
            );
            dag.edge(between(sourceVertex, vEnd).isolated());
            return vEnd;
        }
        return sourceVertex;
    }

    private static TranslationResult<String> translateFilter(RexNode calciteNode, DagBuildContext context,
                                                             ExpressionToMongoVisitor visitor) {
        TranslationResult<String> r;
        try {
            Expression<Boolean> filter = context.convertFilter(calciteNode);
            Object result = visitor.visitGeneric(filter);
            assert result instanceof Bson;
            String expression = ((Bson) result).toBsonDocument(Document.class, defaultCodecRegistry()).toJson();
            r = new TranslationResult<>(expression, true);
        } catch (Throwable t) {
            r = new TranslationResult<>(null, false);
        }
        return r;
    }

    private static TranslationResult<List<String>> translateProjections(List<RexNode> projectionNodes,
                                                                        DagBuildContext context,
                                                                        ExpressionToMongoVisitor visitor) {
        try {
            List<Expression<?>> expressions = context.convertProjection(projectionNodes);
            List<String> fields = expressions.stream()
                                          .map(visitor::visitGeneric)
                                          .filter(proj -> proj instanceof String)
                                          .map(p -> (String) p)
                                          .collect(toList());

            if (fields.size() != projectionNodes.size()) {
                return new TranslationResult<>(emptyList(), false);
            }

            return new TranslationResult<>(fields, true);
        } catch (Throwable t) {
            return new TranslationResult<>(emptyList(), false);
        }
    }

    private static final class TranslationResult<T> {
        final T result;
        final boolean allProceeded;

        private TranslationResult(T result, boolean allProceeded) {
            this.result = result;
            this.allProceeded = allProceeded;
        }
    }
}
