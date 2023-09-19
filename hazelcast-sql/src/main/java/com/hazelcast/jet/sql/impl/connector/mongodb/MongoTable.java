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


import com.hazelcast.jet.mongodb.ResourceChecks;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.JetTable;
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
import static com.hazelcast.jet.mongodb.ResourceChecks.ONCE_PER_JOB;
import static com.hazelcast.jet.mongodb.ResourceChecks.ON_EACH_CONNECT;
import static com.hazelcast.jet.sql.impl.connector.mongodb.Options.CONNECTION_STRING_OPTION;
import static com.hazelcast.jet.sql.impl.connector.mongodb.Options.FORCE_READ_PARALLELISM_ONE;
import static com.hazelcast.jet.sql.impl.connector.mongodb.Options.readExistenceChecksFlag;
import static java.lang.Boolean.parseBoolean;
import static java.util.stream.Collectors.toList;

class MongoTable extends JetTable {

    final String databaseName;
    final String collectionName;
    final String connectionString;
    final String dataConnectionName;
    final Map<String, String> options;
    /**
     * Streaming query always needs _id to be present, even if user don't request it
     */
    final boolean streaming;
    private final String[] externalNames;
    private final QueryDataType[] fieldTypes;
    private final BsonType[] fieldExternalTypes;
    private final boolean forceReadTotalParallelismOne;
    private final ResourceChecks existenceChecks;

    MongoTable(
            @Nonnull String schemaName,
            @Nonnull String name,
            @Nonnull String databaseName,
            @Nullable String collectionName,
            @Nullable String dataConnectionName,
            @Nonnull Map<String, String> options,
            @Nonnull SqlConnector sqlConnector,
            @Nonnull List<TableField> fields,
            @Nonnull TableStatistics statistics,
            @Nonnull String objectType) {
        super(sqlConnector, fields, schemaName, name, statistics, objectType, isStreaming(objectType));
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.options = options;
        this.connectionString = options.get(CONNECTION_STRING_OPTION);
        this.dataConnectionName = dataConnectionName;
        this.streaming = isStreaming(objectType);
        this.existenceChecks = readExistenceChecksFlag(options);

        this.externalNames = getFields().stream()
                                        .map(field -> ((MongoTableField) field).externalName)
                                        .toArray(String[]::new);
        this.fieldTypes = getFields().stream()
                                     .map(TableField::getType)
                                     .toArray(QueryDataType[]::new);
        this.fieldExternalTypes = getFields().stream()
                                             .map(field -> ((MongoTableField) field).externalType)
                                             .toArray(BsonType[]::new);

       this.forceReadTotalParallelismOne = parseBoolean(options.getOrDefault(FORCE_READ_PARALLELISM_ONE, "false"));
    }

    private static boolean isStreaming(String objectType) {
        return "ChangeStream".equalsIgnoreCase(objectType);
    }

    public MongoTableField getField(String name) {
        for (TableField field : getFields()) {
            if (field.getName().equals(name)) {
                return (MongoTableField) field;
            }
        }
        throw new IllegalArgumentException("field " + name + " does not exist");
    }

    String[] externalNames() {
        return externalNames;
    }

    QueryDataType[] fieldTypes() {
        return fieldTypes;
    }

    boolean checkExistenceOnEachCall() {
        return existenceChecks == ONCE_PER_JOB;
    }
    boolean checkExistenceOnEachConnect() {
        return existenceChecks == ON_EACH_CONNECT;
    }

    QueryDataType fieldType(String externalName) {
        for (TableField f : getFields()) {
            MongoTableField mongoTableField = (MongoTableField) f;
            if (mongoTableField.getExternalName().equals(externalName)) {
                return mongoTableField.getType();
            }
        }
        throw new IllegalArgumentException("Unknown column: " + externalName);
    }

    public BsonType[] externalTypes() {
        return fieldExternalTypes;
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

    public boolean isforceReadTotalParallelismOne() {
        return forceReadTotalParallelismOne;
    }

    @Override
    public String toString() {
        return "MongoTable{" +
                "databaseName='" + databaseName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", connectionString='" + connectionString + '\'' +
                ", options=" + options +
                ", streaming=" + streaming +
                ", forceReadTotalParallelismOne=" + forceReadTotalParallelismOne +
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
