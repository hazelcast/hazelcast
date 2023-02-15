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
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.mongodb.impl.Mappers.defaultCodecRegistry;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.stream.Collectors.toList;

/**
 * Base for MongoDB SQL Connectors.
 * <p>
 * Streaming and batch connectors have similar way of dealing with scans with a few exceptions (like the requirement
 * for {@code _id field}).
 * <p>
 * All MongoDB connectors assume one primary key: {@code _id} column, which is mandatory (auto-created if not specified
 * by user), unique and indexed.
 * <p>
 * While secondary indexes are technically possible, they are not supported right now.
 *
 * @see FieldResolver
 */
public abstract class MongoSqlConnectorBase implements SqlConnector {

    private final FieldResolver fieldResolver = new FieldResolver();

    @Nonnull
    @Override
    public List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> userFields,
            @Nonnull String externalName
    ) {
        return fieldResolver.resolveFields(options, userFields, isStream());
    }

    @Nonnull
    @Override
    public List<String> getPrimaryKey(Table table) {
        MongoTable mongoTable = (MongoTable) table;
        return mongoTable.primaryKeyName();
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
        boolean containsId = false;
        for (MappingField resolvedField : resolvedFields) {
            String externalNameFromName = (isStream() ? "fullDocument." : "") + resolvedField.name();
            String fieldExternalName = firstNonNull(resolvedField.externalName(), externalNameFromName);

            if (fieldResolver.isId(fieldExternalName, isStream())) {
                containsId = true;
            }
            fields.add(new MongoTableField(
                    resolvedField.name(),
                    resolvedField.type(),
                    fieldExternalName,
                    resolvedField.isPrimaryKey()
            ));
        }

        if (isStream() && !containsId) {
            fields.add(0, new MongoTableField("fullDocument._id", VARCHAR, "fullDocument._id", true));
        }
        return new MongoTable(schemaName, mappingName, databaseName, collectionName, options, this,
                fields, stats, isStream());
    }

    @Override
    @Nonnull
    public Vertex fullScanReader(
            @Nonnull DagBuildContext context,
            @Nullable RexNode predicate,
            @Nonnull List<RexNode> projection,
            @Nullable FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider) {
        MongoTable table = context.getTable();

        RexToMongoVisitor visitor = new RexToMongoVisitor(table.paths());

        TranslationResult<String> filter = translateFilter(predicate, visitor);
        TranslationResult<List<String>> projections = translateProjections(projection, context, visitor);

        // if not all filters are pushed down, then we cannot push down projection
        // because later filters may use those fields
        // We could do it smarter and check field usage in the filters, but it's something TODO
        List<String> projectionList = filter.allProceeded
                ? projections.result
                : allFieldsExternalNames(table);
        boolean needTwoSteps = !filter.allProceeded || !projections.allProceeded;

        SelectProcessorSupplier supplier;
        if (isStream()) {
            Long startAt = parseStartAt(table.getOptions());
            supplier = new SelectProcessorSupplier(table, filter.result, projectionList, startAt,
                    eventTimePolicyProvider);
        } else {
            supplier = new SelectProcessorSupplier(table, filter.result, projectionList);
        }

        DAG dag = context.getDag();
        Vertex sourceVertex = dag.newUniqueVertex(
                "Select (" + table.getSqlName() + ")", supplier
        );

        if (needTwoSteps) {
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

    private Long parseStartAt(Map<String, String> options) {
        String startAtValue = options.get(Options.START_AT_OPTION);
        if ("now".equalsIgnoreCase(startAtValue)) {
            return System.currentTimeMillis();
        } else {
            Long aLong = asLong(startAtValue);
            if (aLong != null) {
                return aLong;
            } else {
                return Instant.parse(startAtValue).toEpochMilli();
            }
        }
    }

    private static Long asLong(String startAtValue) {
        try {
            return Long.parseLong(startAtValue);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static TranslationResult<String> translateFilter(RexNode filterNode, RexToMongoVisitor visitor) {
        try {
            if (filterNode == null) {
                return new TranslationResult<>(null, true);
            }
            Object result = filterNode.accept(visitor);
            assert result instanceof Bson;

            BsonDocument expression = ((Bson) result).toBsonDocument(BsonDocument.class, defaultCodecRegistry());
            return new TranslationResult<>(expression.toJson(), true);
        } catch (Throwable t) {
            return new TranslationResult<>(null, false);
        }
    }

    private static TranslationResult<List<String>> translateProjections(List<RexNode> projectionNodes,
                                                                        DagBuildContext context,
                                                                        RexToMongoVisitor visitor) {
        try {
            List<String> fields = projectionNodes.stream()
                                          .map(e -> e.accept(visitor))
                                          .filter(proj -> proj instanceof String)
                                          .map(p -> (String) p)
                                          .collect(toList());

            if (fields.isEmpty()) {
                throw new IllegalArgumentException("Projection list cannot be empty");
            }
            if (fields.size() != projectionNodes.size()) {
                return new TranslationResult<>(allFieldsExternalNames(context.getTable()), false);
            }

            return new TranslationResult<>(fields, true);
        } catch (Throwable t) {
            return new TranslationResult<>(allFieldsExternalNames(context.getTable()), false);
        }
    }

    private static List<String> allFieldsExternalNames(MongoTable table) {
        return table.getFields().stream()
                    .map(f -> ((MongoTableField) f).externalName)
                    .collect(toList());
    }

    static final class TranslationResult<T> {
        final T result;
        final boolean allProceeded;

        private TranslationResult(T result, boolean allProceeded) {
            this.result = result;
            this.allProceeded = allProceeded;
        }
    }
}
