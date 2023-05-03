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

import static java.util.Collections.unmodifiableList;

public class JdbcTable extends JetTable {

    private final List<String> dbFieldNames;
    private final List<String> primaryKeyFieldNames;
    private final String[] externalName;
    private final List<String> externalNameList;
    private final String dataConnectionName;
    private final int batchLimit;

    public JdbcTable(
            @Nonnull SqlConnector sqlConnector,
            @Nonnull List<TableField> fields,
            @Nonnull String schemaName,
            @Nonnull String mappingName,
            @Nonnull SqlExternalResource ctx,
            @Nonnull TableStatistics statistics,
            int batchLimit) {

        super(sqlConnector, fields, schemaName, mappingName, statistics, ctx.objectType(), false);

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
        this.externalName = ctx.externalName();
        this.externalNameList = Arrays.asList(externalName);
        this.dataConnectionName = ctx.dataConnection();
        this.batchLimit = batchLimit;
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
        return new JdbcPlanObjectKey();
    }

    static final class JdbcPlanObjectKey implements PlanObjectKey {

    }
}
