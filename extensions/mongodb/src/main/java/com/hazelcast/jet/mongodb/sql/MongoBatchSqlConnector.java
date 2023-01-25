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
package com.hazelcast.jet.mongodb.sql;

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;

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
}
