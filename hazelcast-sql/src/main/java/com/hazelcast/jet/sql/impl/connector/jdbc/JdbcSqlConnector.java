/*
 * Copyright 2021 Hazelcast Inc.
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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactoryImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.resolveTypeForClass;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class JdbcSqlConnector implements SqlConnector {


    public static final String TYPE_NAME = "JDBC";

    public static final String OPTION_EXTERNAL_DATASTORE_REF = "externalDataStoreRef";
    public static final String OPTION_JDBC_BATCH_LIMIT = "jdbc.batch-limit";

    public static final String JDBC_BATCH_LIMIT_DEFAULT_VALUE = "100";

    private static final ILogger LOG = Logger.getLogger(JdbcSqlConnector.class);

    @Override
    public String typeName() {
        return TYPE_NAME;
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
            @Nonnull String externalName) {

        Map<String, DbField> dbFields = readDbFields(nodeEngine, options, externalName);

        List<MappingField> resolvedFields = new ArrayList<>();
        if (userFields.isEmpty()) {
            for (DbField dbField : dbFields.values()) {
                try {
                    Class<?> columnClass = Class.forName(dbField.className);
                    MappingField mappingField = new MappingField(dbField.columnName(), resolveTypeForClass(columnClass));
                    mappingField.setPrimaryKey(dbField.primaryKey());
                    resolvedFields.add(mappingField);
                } catch (ClassNotFoundException e) {
                    throw new IllegalStateException("Could not load column class " + dbField.className, e);
                }
            }
        } else {
            for (MappingField f : userFields) {
                if (f.externalName() != null) {
                    DbField dbField = dbFields.get(f.externalName());
                    if (dbField == null) {
                        throw new IllegalStateException("could not resolve field with external name " + f.externalName());
                    }
                    validateType(f, dbField);
                    MappingField mappingField = new MappingField(f.name(), f.type(), f.externalName());
                    mappingField.setPrimaryKey(dbField.primaryKey());
                    resolvedFields.add(mappingField);
                } else {
                    DbField dbField = dbFields.get(f.name());
                    if (dbField == null) {
                        throw new IllegalStateException("could not resolve field with name " + f.name());
                    }
                    validateType(f, dbField);
                    MappingField mappingField = new MappingField(f.name(), f.type());
                    mappingField.setPrimaryKey(dbField.primaryKey());
                    resolvedFields.add(mappingField);
                }
            }
        }
        return resolvedFields;
    }

    private Map<String, DbField> readDbFields(
            NodeEngine nodeEngine,
            Map<String, String> options,
            String externalTableName) {

        String externalDataStoreRef = requireNonNull(
                options.get(OPTION_EXTERNAL_DATASTORE_REF),
                OPTION_EXTERNAL_DATASTORE_REF + " must be set"
        );
        try {
            DataSource dataSource = (DataSource) nodeEngine.getExternalDataStoreService()
                                                           .getExternalDataStoreFactory(externalDataStoreRef)
                                                           .getDataStore();

            try (Connection connection = dataSource.getConnection();
                 Statement statement = connection.createStatement()) {
                Set<String> pkColumns = readPrimaryKeyColumns(externalTableName, connection);

                boolean hasResultSet = statement.execute("SELECT * FROM " + externalTableName + " LIMIT 0");
                if (!hasResultSet) {
                    throw new IllegalStateException("Could not resolve fields for table " + externalTableName);
                }
                ResultSet rs = statement.getResultSet();
                ResultSetMetaData metaData = rs.getMetaData();

                Map<String, DbField> fields = new HashMap<>();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    fields.put(columnName, new DbField(metaData.getColumnClassName(i),
                            columnName, pkColumns.contains(columnName)));
                }
                return fields;
            }
        } catch (Exception e) {
            throw new HazelcastException("Could not read column metadata for table " + externalDataStoreRef, e);
        }
    }

    private Set<String> readPrimaryKeyColumns(@Nonnull String externalName, Connection connection) {
        Set<String> primaryKeyColumns = new HashSet<>();
        try (ResultSet rs = connection.getMetaData().getPrimaryKeys(null, connection.getSchema(), externalName)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                primaryKeyColumns.add(columnName);
            }
        } catch (SQLException e) {
            throw new HazelcastException("Could not read primary key columns for table " + externalName, e);
        }
        return primaryKeyColumns;
    }

    private void validateType(MappingField field, DbField dbField) {
        try {
            Class<?> columnClassType = Class.forName(dbField.className());
            QueryDataType type = resolveTypeForClass(columnClassType);
            if (!field.type().equals(type) && !type.getConverter().canConvertTo(field.type().getTypeFamily())) {
                throw new IllegalStateException("type " + field.type().getTypeFamily() + " of field " + field.name()
                        + " does not match db type " + type.getTypeFamily());
            }
        } catch (ClassNotFoundException e) {
            throw new HazelcastException("Could not validate type for dbField " + dbField, e);
        }
    }

    @Nonnull
    @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String mappingName,
            @Nonnull String externalName,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> resolvedFields) {

        List<TableField> fields = new ArrayList<>(resolvedFields.size());
        for (MappingField resolvedField : resolvedFields) {
            String fieldExternalName = resolvedField.externalName() != null
                    ? resolvedField.externalName() : resolvedField.name();

            fields.add(new JdbcTableField(
                    resolvedField.name(),
                    resolvedField.type(),
                    fieldExternalName,
                    resolvedField.isPrimaryKey()
            ));
        }

        String externalDataStoreRef = options.get(OPTION_EXTERNAL_DATASTORE_REF);
        SqlDialect dialect = resolveDialect(nodeEngine, externalDataStoreRef);

        return new JdbcTable(
                this,
                fields,
                dialect,
                schemaName,
                mappingName,
                new ConstantTableStatistics(0),
                externalName,
                externalDataStoreRef,
                parseInt(options.getOrDefault(OPTION_JDBC_BATCH_LIMIT, JDBC_BATCH_LIMIT_DEFAULT_VALUE)),
                nodeEngine.getSerializationService()
        );
    }

    private SqlDialect resolveDialect(NodeEngine nodeEngine, String externalDataStoreRef) {
        try {
            DataSource dataSource = (DataSource) nodeEngine.getExternalDataStoreService()
                                                           .getExternalDataStoreFactory(externalDataStoreRef)
                                                           .getDataStore();

            try (Connection connection = dataSource.getConnection()) {

                SqlDialect dialect = SqlDialectFactoryImpl.INSTANCE.create(connection.getMetaData());
                String databaseProductName = connection.getMetaData().getDatabaseProductName();
                switch (databaseProductName) {
                    case "MySQL":
                    case "PostgreSQL":
                    case "H2":
                        return dialect;

                    default:
                        LOG.warning("Database " + databaseProductName + " is not officially supported");
                        return dialect;
                }

            }
        } catch (SQLException e) {
            throw new HazelcastException("Could not determine dialect for externalDataStoreRef: "
                    + externalDataStoreRef, e);
        }
    }


    @Nonnull
    @Override
    public Vertex fullScanReader(
            @Nonnull DAG dag,
            @Nonnull Table table0,
            @Nonnull HazelcastTable hzTable,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projection,
            @Nullable FunctionEx<ExpressionEvalContext,
                    EventTimePolicy<JetSqlRow>> eventTimePolicyProvider) {
        JdbcTable table = (JdbcTable) table0;

        SelectQueryBuilder builder = new SelectQueryBuilder(hzTable);
        return dag.newUniqueVertex(
                "Select (" + table.getExternalName() + ")",
                ProcessorMetaSupplier.forceTotalParallelismOne(
                        new SelectProcessorSupplier(
                                table.getExternalDataStoreRef(),
                                builder.query(),
                                builder.parameterPositions()
                        ))
        );
    }

    @Nonnull
    @Override
    public VertexWithInputConfig insertProcessor(@Nonnull DAG dag, @Nonnull Table table0) {
        JdbcTable table = (JdbcTable) table0;

        InsertQueryBuilder builder = new InsertQueryBuilder(table.getExternalName(), table.dbFieldNames());
        return new VertexWithInputConfig(dag.newUniqueVertex(
                "Insert (" + table.getExternalName() + ")",
                new InsertProcessorSupplier(
                        table.getExternalDataStoreRef(),
                        builder.query(),
                        table.getBatchLimit()
                )
        ));
    }

    @Nonnull
    @Override
    public List<String> getPrimaryKey(Table table0) {
        JdbcTable table = (JdbcTable) table0;
        return table.getPrimaryKeyList();
    }

    @Nonnull
    @Override
    public Vertex updateProcessor(@Nonnull DAG dag,
                                  @Nonnull Table table0,
                                  @Nonnull Map<String, RexNode> updates,
                                  @Nonnull Map<String, Expression<?>> updatesByFieldNames) {
        JdbcTable table = (JdbcTable) table0;

        List<String> pkFields = getPrimaryKey(table0)
                .stream()
                .map(f -> table.getField(f).externalName())
                .collect(toList());

        UpdateQueryBuilder builder = new UpdateQueryBuilder(table, pkFields, updates);

        return dag.newUniqueVertex(
                "Update(" + table.getExternalName() + ")",
                new UpdateProcessorSupplier(
                        table.getExternalDataStoreRef(),
                        builder.query(),
                        builder.parameterPositions(),
                        table.getBatchLimit()
                )
        );
    }

    @Nonnull
    @Override
    public Vertex deleteProcessor(@Nonnull DAG dag, @Nonnull Table table0) {
        JdbcTable table = (JdbcTable) table0;

        List<String> pkFields = getPrimaryKey(table0)
                .stream()
                .map(f -> table.getField(f).externalName())
                .collect(toList());

        DeleteQueryBuilder builder = new DeleteQueryBuilder(table.getExternalName(), pkFields);
        return dag.newUniqueVertex(
                "Delete(" + table.getExternalName() + ")",
                new DeleteProcessorSupplier(
                        table.getExternalDataStoreRef(),
                        builder.query(),
                        table.getBatchLimit()
                )
        );
    }

    private static class DbField {

        private final String className;
        private final String columnName;
        private final boolean primaryKey;

        DbField(String className, String columnName, boolean primaryKey) {
            this.className = requireNonNull(className);
            this.columnName = requireNonNull(columnName);
            this.primaryKey = primaryKey;
        }

        public String className() {
            return className;
        }

        public String columnName() {
            return columnName;
        }

        public boolean primaryKey() {
            return primaryKey;
        }

        @Override
        public String toString() {
            return "DbField{" +
                    "className='" + className + '\'' +
                    ", columnName='" + columnName + '\'' +
                    ", primaryKey=" + primaryKey +
                    '}';
        }
    }
}
