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
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.connector.HazelcastRexNode;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.SqlProcessors;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.apache.calcite.rex.RexNode;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.mongodb.impl.Mappers.bsonToDocument;
import static com.hazelcast.sql.impl.QueryUtils.quoteCompoundIdentifier;
import static com.hazelcast.sql.impl.type.QueryDataType.OBJECT;
import static com.mongodb.client.model.Projections.include;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Base for MongoDB SQL Connectors.
 * <p>
 * Streaming and batch connectors have similar way of dealing with scans with a few exceptions (like the requirement
 * for {@code _id field}).
 * <p>
 * All MongoDB connectors assume at least one primary key.
 * If user didn't specify any, the {@code _id} column is set to primary key - it is mandatory (auto-created if not specified
 * by user), unique and indexed.
 * <p>
 *
 * @see FieldResolver
 */
public abstract class MongoSqlConnectorBase implements SqlConnector {

    @Nonnull
    @Override
    public List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> userFields,
            @Nonnull String[] externalName,
            @Nullable String dataConnectionName) {
        if (externalName.length > 2) {
            throw QueryException.error("Invalid external name " + quoteCompoundIdentifier(externalName)
                    + ", external name for Mongo is allowed to have only one component (collection)"
                    + " or two components (database and collection)");
        }
        FieldResolver fieldResolver = new FieldResolver(nodeEngine);
        return fieldResolver.resolveFields(externalName, dataConnectionName, options, userFields, isStream());
    }

    @Nonnull
    @Override
    public List<String> getPrimaryKey(Table table) {
        MongoTable mongoTable = (MongoTable) table;
        return singletonList(mongoTable.primaryKeyName());
    }

    @Nonnull
    @Override
    public Table createTable(@Nonnull NodeEngine nodeEngine, @Nonnull String schemaName, @Nonnull String mappingName,
                             @Nonnull String[] externalName, @Nullable String dataConnectionName,
                             @Nonnull Map<String, String> options, @Nonnull List<MappingField> resolvedFields) {
        String collectionName = externalName[0]; // TODO HZ-2260
        FieldResolver fieldResolver = new FieldResolver(nodeEngine);
        String databaseName = Options.getDatabaseName(nodeEngine, dataConnectionName, options);
        ConstantTableStatistics stats = new ConstantTableStatistics(0);

        List<TableField> fields = new ArrayList<>(resolvedFields.size());
        boolean containsId = false;
        boolean isStreaming = isStream();
        boolean hasPK = false;
        for (MappingField resolvedField : resolvedFields) {
            String externalNameFromName = (isStreaming ? "fullDocument." : "") + resolvedField.name();
            String fieldExternalName = firstNonNull(resolvedField.externalName(), externalNameFromName);

            if (fieldResolver.isId(fieldExternalName, isStreaming)) {
                containsId = true;
            }
            fields.add(new MongoTableField(
                    resolvedField.name(),
                    resolvedField.type(),
                    fieldExternalName,
                    false,
                    resolvedField.externalType(),
                    resolvedField.isPrimaryKey()));
            hasPK |= resolvedField.isPrimaryKey();
        }

        if (!containsId) {
            if (isStreaming) {
                fields.add(0, new MongoTableField("fullDocument._id", OBJECT, "fullDocument._id",
                        true, "DOCUMENT", !hasPK));
            } else {
                fields.add(0, new MongoTableField("_id", OBJECT, "_id", true,
                        "DOCUMENT", !hasPK));
            }
        }
        return new MongoTable(schemaName, mappingName, databaseName, collectionName, dataConnectionName, options, this,
                fields, stats, isStreaming);
    }

    @Override
    @Nonnull
    public Vertex fullScanReader(
            @Nonnull DagBuildContext context,
            @Nullable HazelcastRexNode predicate,
            @Nonnull List<HazelcastRexNode> projection,
            @Nullable FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider) {
        MongoTable table = context.getTable();

        RexToMongoVisitor visitor = new RexToMongoVisitor();

        TranslationResult<Document> filter = translateFilter(predicate, visitor);
        TranslationResult<List<ProjectionData>> projections = translateProjections(projection, context, visitor);

        // if not all filters are pushed down, then we cannot push down projection
        // because later filters may use those fields
        // We could do it smarter and check field usage in the filters, but it's something TODO

        boolean needTwoSteps = !filter.allProceeded || !projections.allProceeded;
        List<ProjectionData> projectionData = !needTwoSteps
                ? projections.result
                : allFieldsProjection(table);
        ProcessorMetaSupplier supplier;
        if (isStream()) {
            BsonTimestamp startAt = Options.startAt(table.getOptions());
            supplier = wrap(context, new SelectProcessorSupplier(table, filter.result, projectionData, startAt,
                    eventTimePolicyProvider));
        } else {
            supplier = wrap(context, new SelectProcessorSupplier(table, filter.result, projectionData));
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
                            table.externalNames(),
                            table.fieldTypes(),
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

    protected static ProcessorMetaSupplier wrap(DagBuildContext ctx, ProcessorSupplier supplier) {
        MongoTable table = ctx.getTable();
        return table.isForceMongoParallelismOne()
                ? ProcessorMetaSupplier.forceTotalParallelismOne(supplier)
                : ProcessorMetaSupplier.of(supplier);
    }

    @Nonnull
    private static TranslationResult<Document> translateFilter(HazelcastRexNode filterNode, RexToMongoVisitor visitor) {
        if (filterNode == null) {
            return new TranslationResult<>(null, true);
        }
        try {
            Object result = filterNode.unwrap(RexNode.class).accept(visitor);
            boolean isBson = result instanceof Bson;
            assert isBson || result instanceof InputRef;

            if (isBson) {
                return new TranslationResult<>(bsonToDocument((Bson) result), true);
            } else {
                InputRef placeholder = (InputRef) result;
                return new TranslationResult<>(bsonToDocument(Filters.eq(placeholder.asString(), true)), true);
            }
        } catch (UnsupportedOperationException e) {
            return new TranslationResult<>(null, false);
        }
    }

    private static TranslationResult<List<ProjectionData>> translateProjections(
            List<HazelcastRexNode> projectionNodes,
            DagBuildContext context,
            RexToMongoVisitor visitor
    ) {
        List<ProjectionData> projection = new ArrayList<>();
        try {
            MongoTable table = context.getTable();
            String[] externalNames = table.externalNames();
            for (int i = 0; i < projectionNodes.size(); i++) {
                Object translated = projectionNodes.get(i).unwrap(RexNode.class).accept(visitor);
                InputRef ref = InputRef.match(translated);
                if (ref != null) {
                    String externalName = externalNames[ref.getInputIndex()];
                    Document projectionExpr = bsonToDocument(include(externalNames));
                    projection.add(new ProjectionData(externalName, projectionExpr, i, table.fieldType(externalName)));
                } else {
                    // name is not really important, it's by-index at the end
                    Document projectionExpr = new Document("projected_value_" + i, new Document("$literal", translated));
                    projection.add(new ProjectionData("projected_value_" + i, projectionExpr, i, null));
                }
            }

            if (projection.isEmpty()) {
                throw new IllegalArgumentException("Projection list cannot be empty");
            }

            return new TranslationResult<>(projection, true);
        } catch (UnsupportedOperationException e) {
            return new TranslationResult<>(emptyList(), false);
        }
    }

    private static List<ProjectionData> allFieldsProjection(MongoTable table) {
        List<TableField> fields = table.getFields();
        List<ProjectionData> projectionData = new ArrayList<>(fields.size());
        int index = 0;
        for (TableField tableField : fields) {
            MongoTableField field = (MongoTableField) tableField;

            projectionData.add(new ProjectionData(field.externalName,
                    bsonToDocument(Projections.include(field.externalName)), index++, field.getType()));
        }
        return projectionData;
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
