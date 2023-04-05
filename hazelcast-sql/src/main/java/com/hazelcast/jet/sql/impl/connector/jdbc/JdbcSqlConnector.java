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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.datalink.impl.JdbcDataLink;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.connector.HazelcastRexNode;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
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
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.sql.impl.QueryUtils.quoteCompoundIdentifier;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class JdbcSqlConnector implements SqlConnector {

    public static final String TYPE_NAME = "JDBC";

    public static final String OPTION_DATA_LINK_NAME = "data-link-name";
    public static final String OPTION_JDBC_BATCH_LIMIT = "jdbc.batch-limit";

    public static final String JDBC_BATCH_LIMIT_DEFAULT_VALUE = "100";

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
            @Nonnull String[] externalName
    ) {
        if (externalName.length == 0 || externalName.length > 3) {
            throw QueryException.error("Invalid external name " + quoteCompoundIdentifier(externalName)
                    + ", external name for Jdbc must have either 1, 2 or 3 components (catalog, schema and relation)");
        }
        ExternalJdbcTableName externalTableName = new ExternalJdbcTableName(externalName);
        Map<String, DbField> dbFields = readDbFields(nodeEngine, options, externalTableName);

        List<MappingField> resolvedFields = new ArrayList<>();
        if (userFields.isEmpty()) {
            for (DbField dbField : dbFields.values()) {
                try {
                    MappingField mappingField = new MappingField(
                            dbField.columnName,
                            resolveType(dbField.columnTypeName)
                    );
                    mappingField.setPrimaryKey(dbField.primaryKey);
                    resolvedFields.add(mappingField);
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException("Could not load column class " + dbField.columnTypeName, e);
                }
            }
        } else {
            for (MappingField f : userFields) {
                if (f.externalName() != null) {
                    DbField dbField = dbFields.get(f.externalName());
                    if (dbField == null) {
                        throw new IllegalStateException("Could not resolve field with external name " + f.externalName());
                    }
                    validateType(f, dbField);
                    MappingField mappingField = new MappingField(f.name(), f.type(), f.externalName(), dbField.columnTypeName);
                    mappingField.setPrimaryKey(dbField.primaryKey);
                    resolvedFields.add(mappingField);
                } else {
                    DbField dbField = dbFields.get(f.name());
                    if (dbField == null) {
                        throw new IllegalStateException("Could not resolve field with name " + f.name());
                    }
                    validateType(f, dbField);
                    MappingField mappingField = new MappingField(f.name(), f.type());
                    mappingField.setPrimaryKey(dbField.primaryKey);
                    resolvedFields.add(mappingField);
                }
            }
        }
        return resolvedFields;
    }

    private Map<String, DbField> readDbFields(
            NodeEngine nodeEngine,
            Map<String, String> options,
            ExternalJdbcTableName externalTableName
    ) {
        String dataLinkRef = requireNonNull(
                options.get(OPTION_DATA_LINK_NAME),
                "Missing option: '" + OPTION_DATA_LINK_NAME + "' must be set"
        );
        JdbcDataLink dataLink = getAndRetainDataLink(nodeEngine, dataLinkRef);
        try (Connection connection = dataLink.getConnection()) {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            Set<String> pkColumns = readPrimaryKeyColumns(externalTableName, databaseMetaData);
            return readColumns(externalTableName, databaseMetaData, pkColumns);
        } catch (Exception e) {
            throw new HazelcastException("Could not execute readDbFields for table " + externalTableName, e);
        } finally {
            dataLink.release();
        }
    }

    private static Set<String> readPrimaryKeyColumns(ExternalJdbcTableName externalTableName, DatabaseMetaData databaseMetaData) {
        Set<String> pkColumns = new HashSet<>();
        try (ResultSet resultSet = databaseMetaData.getPrimaryKeys(
                externalTableName.catalog,
                externalTableName.schema,
                externalTableName.table)
        ) {
            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                pkColumns.add(columnName);
            }
        } catch (SQLException e) {
            throw new HazelcastException("Could not read primary key columns for table " + externalTableName, e);
        }
        return pkColumns;
    }

    private static Map<String, DbField> readColumns(ExternalJdbcTableName externalTableName, DatabaseMetaData databaseMetaData,
                                                    Set<String> pkColumns) {
        Map<String, DbField> fields = new LinkedHashMap<>();
        try (ResultSet resultSet = databaseMetaData.getColumns(
                externalTableName.catalog,
                externalTableName.schema,
                externalTableName.table,
                null)) {
            while (resultSet.next()) {
                String columnTypeName = resultSet.getString("TYPE_NAME");
                String columnName = resultSet.getString("COLUMN_NAME");
                fields.put(columnName,
                        new DbField(columnTypeName,
                                columnName,
                                pkColumns.contains(columnName)
                        ));
            }
        } catch (SQLException e) {
            throw new HazelcastException("Could not read columns for table " + externalTableName, e);
        }
        return fields;
    }

    private static JdbcDataLink getAndRetainDataLink(NodeEngine nodeEngine, String dataLinkName) {
        return nodeEngine
                .getDataLinkService()
                .getAndRetainDataLink(dataLinkName, JdbcDataLink.class);
    }

    private void validateType(MappingField field, DbField dbField) {
        QueryDataType type = resolveType(dbField.columnTypeName);
        if (!field.type().equals(type) && !type.getConverter().canConvertTo(field.type().getTypeFamily())) {
            throw new IllegalStateException("Type " + field.type().getTypeFamily() + " of field " + field.name()
                                            + " does not match db type " + type.getTypeFamily());
        }
    }

    @Nonnull
    @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String mappingName,
            @Nonnull String[] externalName,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> resolvedFields
    ) {
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

        String dataLinkName = options.get(OPTION_DATA_LINK_NAME);
        SqlDialect dialect = resolveDialect(nodeEngine, dataLinkName);

        return new JdbcTable(
                this,
                fields,
                dialect,
                schemaName,
                mappingName,
                new ConstantTableStatistics(0),
                externalName,
                dataLinkName,
                parseInt(options.getOrDefault(OPTION_JDBC_BATCH_LIMIT, JDBC_BATCH_LIMIT_DEFAULT_VALUE)),
                nodeEngine.getSerializationService()
        );
    }

    private SqlDialect resolveDialect(NodeEngine nodeEngine, String dataLinkRef) {
        JdbcDataLink dataLink = getAndRetainDataLink(nodeEngine, dataLinkRef);
        try (Connection connection = dataLink.getConnection()) {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            SqlDialect dialect = SqlDialectFactoryImpl.INSTANCE.create(databaseMetaData);
            SupportedDatabases.logOnceIfDatabaseNotSupported(databaseMetaData);
            return dialect;
        } catch (Exception e) {
            throw new HazelcastException("Could not determine dialect for dataLinkRef: " + dataLinkRef, e);
        } finally {
            dataLink.release();
        }
    }

    @Nonnull
    @Override
    public Vertex fullScanReader(
            @Nonnull DagBuildContext context,
            @Nullable HazelcastRexNode predicate,
            @Nonnull List<HazelcastRexNode> projection,
            @Nullable FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider
    ) {
        if (eventTimePolicyProvider != null) {
            throw QueryException.error("Ordering functions are not supported on top of " + TYPE_NAME + " mappings");
        }
        JdbcTable table = context.getTable();

        List<RexNode> projections = Util.toList(projection, n -> n.unwrap(RexNode.class));
        RexNode filter = predicate == null ? null : predicate.unwrap(RexNode.class);
        SelectQueryBuilder builder = new SelectQueryBuilder(context.getTable(), filter, projections);
        return context.getDag().newUniqueVertex(
                "Select(" + table.getExternalNameList() + ")",
                ProcessorMetaSupplier.forceTotalParallelismOne(
                        new SelectProcessorSupplier(
                                table.getDataLinkName(),
                                builder.query(),
                                builder.parameterPositions()
                        ))
        );
    }

    @Nonnull
    @Override
    public VertexWithInputConfig insertProcessor(@Nonnull DagBuildContext context) {
        JdbcTable table = context.getTable();

        InsertQueryBuilder builder = new InsertQueryBuilder(table);
        return new VertexWithInputConfig(context.getDag().newUniqueVertex(
                "Insert(" + table.getExternalNameList() + ")",
                new InsertProcessorSupplier(
                        table.getDataLinkName(),
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
    public Vertex updateProcessor(
            @Nonnull DagBuildContext context,
            @Nonnull List<String> fieldNames,
            @Nonnull List<HazelcastRexNode> expressions
    ) {
        JdbcTable table = context.getTable();

        List<String> pkFields = getPrimaryKey(context.getTable())
                .stream()
                .map(f -> table.getField(f).externalName())
                .collect(toList());

        List<RexNode> projections = Util.toList(expressions, n -> n.unwrap(RexNode.class));
        UpdateQueryBuilder builder = new UpdateQueryBuilder(table, pkFields, fieldNames, projections);

        return context.getDag().newUniqueVertex(
                "Update(" + table.getExternalNameList() + ")",
                new UpdateProcessorSupplier(
                        table.getDataLinkName(),
                        builder.query(),
                        builder.parameterPositions(),
                        table.getBatchLimit()
                )
        );
    }

    @Nonnull
    @Override
    public Vertex deleteProcessor(@Nonnull DagBuildContext context) {
        JdbcTable table = context.getTable();

        List<String> pkFields = getPrimaryKey(context.getTable())
                .stream()
                .map(f -> table.getField(f).externalName())
                .collect(toList());

        DeleteQueryBuilder builder = new DeleteQueryBuilder(table, pkFields);
        return context.getDag().newUniqueVertex(
                "Delete(" + table.getExternalNameList() + ")",
                new DeleteProcessorSupplier(
                        table.getDataLinkName(),
                        builder.query(),
                        table.getBatchLimit()
                )
        );
    }

    @Nonnull
    @Override
    public Vertex sinkProcessor(@Nonnull DagBuildContext context) {
        JdbcTable jdbcTable = context.getTable();

        // If dialect is supported
        if (SupportedDatabases.isDialectSupported(jdbcTable)) {
            // Get the upsert statement
            String upsertStatement = UpsertBuilder.getUpsertStatement(jdbcTable);

            // Create Vertex with the UPSERT statement
            return context.getDag().newUniqueVertex(
                    "sinkProcessor(" + jdbcTable.getExternalNameList() + ")",
                    new UpsertProcessorSupplier(
                            jdbcTable.getDataLinkName(),
                            upsertStatement,
                            jdbcTable.getBatchLimit()
                    )
            );
        }
        // Unsupported dialect. Create Vertex with the INSERT statement
        VertexWithInputConfig vertexWithInputConfig = insertProcessor(context);
        return vertexWithInputConfig.vertex();
    }

    /**
     * Using {@link ResultSetMetaData#getColumnTypeName(int)} seems more
     * reliable than {@link ResultSetMetaData#getColumnClassName(int)},
     * which doesn't allow to distinguish between timestamp and timestamp
     * with time zone, or between tinyint/smallint and int for some JDBC drivers.
     */
    @SuppressWarnings("ReturnCount")
    private QueryDataType resolveType(String columnTypeName) {
        switch (columnTypeName.toUpperCase()) {
            case "BOOLEAN":
            case "BOOL":
            case "BIT":
                return QueryDataType.BOOLEAN;

            case "VARCHAR":
            case "CHARACTER VARYING":
                return QueryDataType.VARCHAR;

            case "TINYINT":
                return QueryDataType.TINYINT;

            case "SMALLINT":
            case "INT2":
                return QueryDataType.SMALLINT;

            case "INT":
            case "INT4":
            case "INTEGER":
                return QueryDataType.INT;

            case "INT8":
            case "BIGINT":
                return QueryDataType.BIGINT;

            case "DECIMAL":
            case "NUMERIC":
                return QueryDataType.DECIMAL;

            case "REAL":
            case "FLOAT":
            case "FLOAT4":
                return QueryDataType.REAL;

            case "DOUBLE":
            case "DOUBLE PRECISION":
            case "FLOAT8":
                return QueryDataType.DOUBLE;

            case "DATE":
                return QueryDataType.DATE;

            case "TIME":
                return QueryDataType.TIME;

            case "TIMESTAMP":
                return QueryDataType.TIMESTAMP;

            case "TIMESTAMP WITH TIME ZONE":
                return QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;

            default:
                throw new IllegalArgumentException("Unknown column type: " + columnTypeName);
        }
    }

    private static class DbField {

        final String columnTypeName;
        final String columnName;
        final boolean primaryKey;

        DbField(String columnTypeName, String columnName, boolean primaryKey) {
            this.columnTypeName = requireNonNull(columnTypeName);
            this.columnName = requireNonNull(columnName);
            this.primaryKey = primaryKey;
        }

        @Override
        public String toString() {
            return "DbField{" +
                   "name='" + columnName + '\'' +
                   ", typeName='" + columnTypeName + '\'' +
                   ", primaryKey=" + primaryKey +
                   '}';
        }
    }

    private static class ExternalJdbcTableName {

        final String catalog;
        final String schema;
        final String table;

        ExternalJdbcTableName(String[] externalName) {
            if (externalName.length == 1) {
                catalog = null;
                schema = null;
                table = externalName[0];
            } else if (externalName.length == 2) {
                catalog = null;
                schema = externalName[0];
                table = externalName[1];
            } else if (externalName.length == 3) {
                catalog = externalName[0];
                schema = externalName[1];
                table = externalName[2];
            } else {
                // external name length was validater earlier, we should never get here
                throw new IllegalStateException("Invalid external name length");
            }
        }
    }
}
