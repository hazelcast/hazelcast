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
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MongoTable extends JetTable {

    private final String databaseName;
    private final String collectionName;
    private final Map<String, String> options;

    public MongoTable(
            @Nonnull String schemaName,
            @Nonnull String name,
            @Nonnull String databaseName,
            @Nullable String collectionName,
            @Nonnull Map<String, String> options,
            @Nonnull SqlConnector sqlConnector,
            @Nonnull List<TableField> fields,
            @Nonnull TableStatistics statistics) {
        super(sqlConnector, fields, schemaName, name, statistics);
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.options = options;
    }

    @Override
    public PlanObjectKey getObjectKey() {
        return new MongoObjectKey(getSchemaName(), getSqlName(), databaseName, collectionName, getFields(), options);
    }

    static final class MongoObjectKey implements PlanObjectKey {
        private final String schemaName;
        private final String tableName;
        private final String databaseName;
        private final String collectionName;
        private final List<TableField> fields;
        private final Map<String, String> options;

        MongoObjectKey(String schemaName, String tableName, String databaseName, String collectionName,
                       List<TableField> fields, Map<String, String> options) {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.databaseName = databaseName;
            this.collectionName = collectionName;
            this.fields = fields;
            this.options = options;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MongoObjectKey that = (MongoObjectKey) o;
            return Objects.equals(databaseName, that.databaseName) && Objects.equals(collectionName,
                    that.collectionName) && Objects.equals(fields, that.fields) && Objects.equals(options,
                    that.options);
        }

        @Override
        public int hashCode() {
            return Objects.hash(databaseName, collectionName, fields, options);
        }

        @Override
        public String toString() {
            return "MongoObjectKey{" +
                    "databaseName='" + databaseName + '\'' +
                    ", collectionName='" + collectionName + '\'' +
                    ", fields=" + fields +
                    ", options=" + options +
                    '}';
        }
    }
}
