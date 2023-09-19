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

import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.mongodb.WriteMode;
import com.hazelcast.jet.mongodb.impl.Mappers;
import com.hazelcast.jet.sql.impl.connector.HazelcastRexNode;
import com.mongodb.client.model.Filters;
import org.apache.calcite.rex.RexNode;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;

import static com.hazelcast.jet.mongodb.impl.MongoUtilities.UPDATE_ALL_PREDICATE;
import static java.util.stream.Collectors.toList;

/**
 * Batch-query version of MongoDB SQL Connector.
 *
 * @see MongoSqlConnectorBase
 * @see FieldResolver
 */
public class MongoSqlConnector extends MongoSqlConnectorBase {

    public static final String TYPE_NAME = "Mongo";

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Nonnull
    @Override
    public VertexWithInputConfig insertProcessor(@Nonnull DagBuildContext context) {
        Vertex vertex = context.getDag().newUniqueVertex(
                "Insert(" + context.getTable().getSqlName() + ")",
                new InsertProcessorSupplier(context.getTable(), WriteMode.INSERT_ONLY)
        );
        return new VertexWithInputConfig(vertex);
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
        MongoTable table = context.getTable();
        RexToMongoVisitor visitor = new RexToMongoVisitor();
        List<? extends Serializable> updates = expressions.stream()
                                                          .map(e -> e.unwrap(RexNode.class).accept(visitor))
                                                          .map(doc -> {
                                                              assert doc instanceof Serializable;
                                                              return (Serializable) doc;
                                                          })
                                                          .collect(toList());

        String[] fieldNamesArray = fieldNames.toArray(String[]::new);
        if (hasInput) {
            return context.getDag().newUniqueVertex(
                    "Update(" + table.getSqlName() + ")",
                    wrap(context, new UpdateProcessorSupplier(table, fieldNamesArray, updates, null, hasInput))
            );
        } else {
            Object predicateRaw = predicate == null
                    ? Filters.empty()
                    : predicate.unwrap(RexNode.class).accept(visitor);
            Serializable translated = predicateRaw instanceof Bson
                    ? Mappers.bsonToDocument((Bson) predicateRaw)
                    : (Serializable) predicateRaw;

            return context.getDag().newUniqueVertex(
                    "Update(" + table.getSqlName() + ")",
                    wrapWithParallelismOne(context,
                        new UpdateProcessorSupplier(table, fieldNamesArray, updates, translated, hasInput)
                    )
            );
        }
    }

    @Nonnull
    @Override
    public Vertex sinkProcessor(@Nonnull DagBuildContext context) {
        MongoTable table = context.getTable();

        return context.getDag().newUniqueVertex(
                "Sink(" + table.getSqlName() + ")",
                wrap(context, new InsertProcessorSupplier(table, WriteMode.UPSERT))
        );
    }

    @Nonnull
    @Override
    public Vertex deleteProcessor(
            @Nonnull DagBuildContext context,
            @Nullable HazelcastRexNode predicate,
            boolean hasInput
    ) {
        MongoTable table = context.getTable();

        if (hasInput) {
            return context.getDag().newUniqueVertex(
                    "Delete(" + table.getSqlName() + ")",
                    wrap(context, new DeleteProcessorSupplier(table, null, hasInput))
            );
        } else {
            Object predicateTranslated = predicate == null
                    ? UPDATE_ALL_PREDICATE
                    : predicate.unwrap(RexNode.class).accept(new RexToMongoVisitor());
            Serializable predicateToSend;
            if (predicateTranslated instanceof Bson) {
                predicateToSend = Mappers.bsonToDocument((Bson) predicateTranslated);
            } else {
                assert predicateTranslated instanceof Serializable;
                predicateToSend = (Serializable) predicateTranslated;
            }

            return context.getDag().newUniqueVertex(
                    "Delete(" + table.getSqlName() + ")",
                    wrapWithParallelismOne(context, new DeleteProcessorSupplier(table, predicateToSend, hasInput))
            );
        }
    }
}
