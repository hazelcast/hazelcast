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

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;
import org.apache.calcite.sql.SqlDialect;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public class JdbcTable extends JetTable {

    private final List<String> dbFieldNames;
    private final List<String> primaryKeyFieldNames;
    private final SqlDialect sqlDialect;
    private final String externalName;
    private final String externalDataStoreRef;
    private final int batchLimit;
    private final SerializationService serializationService;

    public JdbcTable(
            @Nonnull SqlConnector sqlConnector,
            @Nonnull List<TableField> fields,
            @Nonnull SqlDialect dialect,
            @Nonnull String schemaName,
            @Nonnull String name,
            @Nonnull TableStatistics statistics,
            @Nonnull String externalName,
            @Nonnull String externalDataStoreRef,
            int batchLimit,
            @Nonnull SerializationService serializationService) {

        super(sqlConnector, fields, schemaName, name, statistics);

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
        this.sqlDialect = dialect;
        this.externalName = externalName;
        this.externalDataStoreRef = externalDataStoreRef;
        this.batchLimit = batchLimit;
        this.serializationService = serializationService;
    }

    public List<String> dbFieldNames() {
        return dbFieldNames;
    }

    public SqlDialect sqlDialect() {
        return sqlDialect;
    }

    public String getExternalName() {
        return externalName;
    }

    public String getExternalDataStoreRef() {
        return externalDataStoreRef;
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

    public SerializationService getSerializationService() {
        return serializationService;
    }

    @Override
    public PlanObjectKey getObjectKey() {
        return new JdbcPlanObjectKey();
    }

    static final class JdbcPlanObjectKey implements PlanObjectKey {

    }
}
