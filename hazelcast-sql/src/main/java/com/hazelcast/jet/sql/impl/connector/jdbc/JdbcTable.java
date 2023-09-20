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

package com.hazelcast.jet.sql.impl.connector.jdbc;

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.SqlConnector.SqlExternalResource;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.lang.Integer.parseInt;
import static java.util.Collections.unmodifiableList;

public class JdbcTable extends JetTable {

    public static final String OPTION_JDBC_BATCH_LIMIT = "jdbc.batch-limit";
    public static final String JDBC_BATCH_LIMIT_DEFAULT_VALUE = "100";

    private final List<String> dbFieldNames;
    private final List<String> primaryKeyFieldNames;
    private final String[] externalName;
    private final List<String> externalNameList;
    private final String dataConnectionName;
    private final Map<String, String> options;
    private final int batchLimit;

    public JdbcTable(
            @Nonnull SqlConnector sqlConnector,
            @Nonnull List<TableField> fields,
            @Nonnull String schemaName,
            @Nonnull String mappingName,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull TableStatistics statistics) {

        super(sqlConnector, fields, schemaName, mappingName, statistics, externalResource.objectType(), false);

        List<String> dbFieldNames = new ArrayList<>(fields.size());
        List<String> primaryKeyFieldNames = new ArrayList<>(1);

        for (TableField field : fields) {
            JdbcTableField jdbcField = (JdbcTableField) field;
            dbFieldNames.add(jdbcField.externalName());
            if (jdbcField.isPrimaryKey()) {
                primaryKeyFieldNames.add(jdbcField.getName());
            }
        }

        this.dbFieldNames = unmodifiableList(dbFieldNames);
        this.primaryKeyFieldNames = unmodifiableList(primaryKeyFieldNames);
        this.externalName = externalResource.externalName();
        this.externalNameList = Arrays.asList(externalName);
        this.dataConnectionName = externalResource.dataConnection();
        this.options = externalResource.options();
        this.batchLimit = parseInt(options.getOrDefault(OPTION_JDBC_BATCH_LIMIT, JDBC_BATCH_LIMIT_DEFAULT_VALUE));
    }

    public List<String> dbFieldNames() {
        return dbFieldNames;
    }

    public String[] getExternalName() {
        return externalName;
    }

    public List<String> getExternalNameList() {
        return externalNameList;
    }

    public String getDataConnectionName() {
        return dataConnectionName;
    }

    public int getBatchLimit() {
        return batchLimit;
    }

    public JdbcTableField getField(String fieldName) {
        List<TableField> fields = getFields();
        for (TableField field : fields) {
            if (field.getName().equals(fieldName)) {
                return (JdbcTableField) field;
            }
        }
        throw new IllegalArgumentException("Unknown field with name " + fieldName);
    }

    public JdbcTableField getFieldByExternalName(String externalName) {
        List<TableField> fields = getFields();
        for (TableField field0 : fields) {
            JdbcTableField field = (JdbcTableField) field0;

            if (field.externalName().equals(externalName)) {
                return field;
            }
        }
        throw new IllegalArgumentException("Unknown field with name " + externalName);
    }

    public List<String> getPrimaryKeyList() {
        return primaryKeyFieldNames;
    }

    @Override
    public PlanObjectKey getObjectKey() {
        return new JdbcPlanObjectKey(getSchemaName(), getSqlName(), externalName, dataConnectionName, getFields(),
                options);
    }

    static final class JdbcPlanObjectKey implements PlanObjectKey {

        private final String schemaName;
        private final String tableName;
        private final String[] externalName;
        private final String dataConnectionName;
        private final List<TableField> fields;
        private final Map<String, String> options;


        JdbcPlanObjectKey(String schemaName, String tableName, String[] externalName, String dataConnectionName,
                          List<TableField> fields, Map<String, String> options) {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.externalName = externalName;
            this.dataConnectionName = dataConnectionName;
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
            JdbcPlanObjectKey that = (JdbcPlanObjectKey) o;
            return Objects.equals(schemaName, that.schemaName)
                    && Objects.equals(tableName, that.tableName)
                    && Arrays.equals(externalName, that.externalName)
                    && Objects.equals(dataConnectionName, that.dataConnectionName)
                    && Objects.equals(fields, that.fields)
                    && Objects.equals(options, that.options);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(schemaName, tableName, dataConnectionName, fields, options);
            result = 31 * result + Arrays.hashCode(externalName);
            return result;
        }
    }
}
