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


import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.bson.BsonType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.toList;

class MongoTable extends JetTable {

    final String databaseName;
    final String collectionName;
    final String connectionString;
    final String dataLinkName;
    final Map<String, String> options;
    /**
     * Streaming query always needs _id to be present, even if user don't request it
     */
    final boolean streaming;
    private final String[] externalNames;
    private final QueryDataType[] fieldTypes;
    private final BsonType[] fieldExternalTypes;

    MongoTable(
            @Nonnull String schemaName,
            @Nonnull String name,
            @Nonnull String databaseName,
            @Nullable String collectionName,
            @Nonnull Map<String, String> options,
            @Nonnull SqlConnector sqlConnector,
            @Nonnull List<TableField> fields,
            @Nonnull TableStatistics statistics,
            boolean streaming) {
        super(sqlConnector, fields, schemaName, name, statistics);
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.options = options;
        this.connectionString = options.get(Options.CONNECTION_STRING_OPTION);
        this.dataLinkName = options.get(Options.DATA_LINK_REF_OPTION);
        this.streaming = streaming;

        this.externalNames = getFields().stream()
                                        .map(field -> ((MongoTableField) field).externalName)
                                        .toArray(String[]::new);
        this.fieldTypes = getFields().stream()
                                     .map(TableField::getType)
                                     .toArray(QueryDataType[]::new);
        this.fieldExternalTypes = getFields().stream()
                                             .map(field -> ((MongoTableField) field).externalType)
                                             .toArray(BsonType[]::new);
    }

    public MongoTableField getField(String name) {
        for (TableField field : getFields()) {
            if (field.getName().equals(name)) {
                return (MongoTableField) field;
            }
        }
        throw new IllegalArgumentException("field " + name + " does not exist");
    }

    public MongoTableField getFieldByExternalName(String name) {
        return getFields().stream()
                          .map(f -> (MongoTableField) f)
                          .filter(f -> f.getExternalName().equals(name))
                          .findFirst()
                          .orElseThrow(() -> new IllegalArgumentException("field " + name + " does not exist"));
    }

    String[] externalNames() {
        return externalNames;
    }

    QueryDataType[] fieldTypes() {
        return fieldTypes;
    }

    public BsonType[] externalTypes() {
        return fieldExternalTypes;
    }

    QueryDataType pkType() {
        List<QueryDataType> list = getFields().stream()
                                       .filter(field -> ((MongoTableField) field).isPrimaryKey())
                                       .map(TableField::getType)
                                       .collect(toList());
        checkState(list.size() == 1, "there should be exactly 1 primary key, got: " + list);
        return list.get(0);
    }
    BsonType pkExternalType() {
        List<BsonType> list = getFields().stream()
                                       .filter(field -> ((MongoTableField) field).isPrimaryKey())
                                       .map(field -> ((MongoTableField) field).getExternalType())
                                       .collect(toList());
        checkState(list.size() == 1, "there should be exactly 1 primary key, got: " + list);
        return list.get(0);
    }

    SupplierEx<QueryTarget> queryTargetSupplier() {
        List<String> fields = getFields().stream()
                                          .map(f -> ((MongoTableField) f).externalName)
                                          .collect(toList());
        return () -> new DocumentQueryTarget(fields);
    }

    @Override
    public PlanObjectKey getObjectKey() {
        return new MongoObjectKey(getSchemaName(), getSqlName(), databaseName, collectionName, getFields(), options);
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public String primaryKeyName() {
        List<String> list = getFields().stream()
                                     .filter(field -> ((MongoTableField) field).isPrimaryKey())
                                     .map(TableField::getName)
                                     .collect(toList());
        checkState(list.size() == 1, "there should be exactly 1 primary key, got: " + list);
        return list.get(0);
    }

    public String primaryKeyExternalName() {
        List<String> list = getFields().stream()
                                     .filter(field -> ((MongoTableField) field).isPrimaryKey())
                                     .map(field -> ((MongoTableField) field).externalName)
                                     .collect(toList());
        checkState(list.size() == 1, "there should be exactly 1 primary key, got: " + list);
        return list.get(0);
    }

    /**
     * Returns an array of data types with order the same as the order of fields in the projection.
     */
    @Nonnull
    QueryDataType[] resolveColumnTypes(@Nonnull List<String> requestedProjection) {
        QueryDataType[] types = new QueryDataType[requestedProjection.size()];
        int i = 0;
        for (String column : requestedProjection) {
            types[i++] = getFieldByExternalName(column).getType();
        }
        return types;
    }

    @Override
    public String toString() {
        return "MongoTable{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", connectionString='" + connectionString + '\'' +
                ", options=" + options +
                ", streaming=" + streaming +
                '}';
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
            if (!(o instanceof MongoObjectKey)) {
                return false;
            }
            MongoObjectKey that = (MongoObjectKey) o;
            return Objects.equals(schemaName, that.schemaName)
                    && Objects.equals(tableName, that.tableName)
                    && Objects.equals(databaseName, that.databaseName)
                    && Objects.equals(collectionName, that.collectionName)
                    && Objects.equals(fields, that.fields)
                    && Objects.equals(options, that.options);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schemaName, tableName, databaseName, collectionName, fields, options);
        }
    }
}
