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
import org.apache.calcite.rex.RexNode;

import javax.annotation.Nonnull;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Batch-query version of  MongoDB SQL Connector.
 *
 * {@inheritDoc}
 *
 * @see FieldResolver
 */
public class MongoBatchSqlConnector extends MongoSqlConnectorBase {

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
    public VertexWithInputConfig insertProcessor(@Nonnull DagBuildContext context) {
        Vertex vertex = context.getDag().newUniqueVertex(
                "Insert(" + context.getTable().getSqlName() + ")",
                new InsertProcessorSupplier(context.getTable(), WriteMode.INSERT_ONLY)
        );
        return new VertexWithInputConfig(vertex);
    }

    @Nonnull
    @Override
    public Vertex updateProcessor(@Nonnull DagBuildContext context,
                                  @Nonnull List<String> fieldNames,
                                  @Nonnull List<RexNode> expressions) {
        MongoTable table = context.getTable();
        RexToMongoVisitor visitor = new RexToMongoVisitor(table.paths());
        List<Object> updates = expressions.stream()
                                          .map(e -> e.accept(visitor))
                                          .collect(toList());

        Vertex vertex = context.getDag().newUniqueVertex(
                "Update(" + table.getSqlName() + ")",
                new UpdateProcessorSupplier(table, fieldNames, updates)
        );
        return vertex;
    }

    @Nonnull
    @Override
    public Vertex sinkProcessor(@Nonnull DagBuildContext context) {
        MongoTable table = context.getTable();

        Vertex vertex = context.getDag().newUniqueVertex(
                "Update(" + table.getSqlName() + ")",
                new InsertProcessorSupplier(table, WriteMode.UPSERT)
        );
        return vertex;
    }

}
