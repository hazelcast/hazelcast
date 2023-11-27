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

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.dataconnection.impl.DatabaseDialect;
import com.hazelcast.dataconnection.impl.JdbcDataConnection;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.HazelcastRexNode;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.jdbc.mssql.HazelcastMSSQLDialect;
import com.hazelcast.jet.sql.impl.connector.jdbc.mysql.HazelcastMySqlDialect;
import com.hazelcast.jet.sql.impl.connector.jdbc.oracle.HazelcastOracleDialect;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryException;
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
import org.apache.calcite.sql.SqlDialects;

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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;
import static com.hazelcast.jet.core.ProcessorSupplier.of;
import static com.hazelcast.jet.sql.impl.connector.jdbc.GettersProvider.GETTERS;
import static com.hazelcast.sql.impl.QueryUtils.quoteCompoundIdentifier;
import static java.util.Objects.requireNonNull;

public class JdbcSqlConnector implements SqlConnector {

    public static final String TYPE_NAME = "JDBC";

    private static final JetSqlRow DUMMY_INPUT_ROW = new JetSqlRow(null, new Object[]{});

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Nonnull
    @Override
    public String defaultObjectType() {
        return JdbcDataConnection.OBJECT_TYPE_TABLE;
    }

    @Nonnull
    @Override
    public List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> userFields) {
        if (externalResource.dataConnection() == null) {
            throw QueryException.error("You must provide data connection when using the Jdbc connector");
        }
        ExternalJdbcTableName.validateExternalName(externalResource.externalName());

        JdbcDataConnection dataConnection = nodeEngine.getDataConnectionService().getAndRetainDataConnection(
                externalResource.dataConnection(), JdbcDataConnection.class);
        try (Connection connection = dataConnection.getConnection()) {
            TypeResolver typeResolver = typeResolver(connection);

            Map<String, DbField> dbFields = readDbFields(connection, externalResource.externalName());

            List<MappingField> resolvedFields = new ArrayList<>();
            if (userFields.isEmpty()) {
                for (DbField dbField : dbFields.values()) {
                    MappingField mappingField = new MappingField(
                            dbField.columnName,
                            typeResolver.resolveType(dbField.columnTypeName, dbField.precision, dbField.scale)
                    );
                    mappingField.setPrimaryKey(dbField.primaryKey);
                    resolvedFields.add(mappingField);
                }
            } else {
                for (MappingField f : userFields) {
                    if (f.externalName() != null) {
                        DbField dbField = dbFields.get(f.externalName());
                        if (dbField == null) {
                            throw QueryException.error("Could not resolve field with external name " + f.externalName());
                        }
                        validateType(typeResolver, f, dbField);
                        MappingField mappingField = new MappingField(f.name(), f.type(), f.externalName(),
                                dbField.columnTypeName);
                        mappingField.setPrimaryKey(dbField.primaryKey);
                        resolvedFields.add(mappingField);
                    } else {
                        DbField dbField = dbFields.get(f.name());
                        if (dbField == null) {
                            throw QueryException.error("Could not resolve field with name " + f.name());
                        }
                        validateType(typeResolver, f, dbField);
                        MappingField mappingField = new MappingField(f.name(), f.type());
                        mappingField.setPrimaryKey(dbField.primaryKey);
                        resolvedFields.add(mappingField);
                    }
                }
            }
            return resolvedFields;
        } catch (SQLException e) {
            throw new HazelcastSqlException("Could not resolve and validate fields", e);
        } finally {
            dataConnection.release();
        }
    }

    static TypeResolver typeResolver(Connection connection) {
        try {
            SqlDialect dialect = resolveDialect(connection.getMetaData());
            if (dialect instanceof TypeResolver) {
                return (TypeResolver) dialect;
            } else {
                return DefaultTypeResolver::resolveType;
            }
        } catch (SQLException e) {
            throw new HazelcastSqlException("Could not create type resolver", e);
        }
    }

    private Map<String, DbField> readDbFields(
            Connection connection,
            String[] externalName
    ) {

        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();

            ExternalJdbcTableName externalTableName = new ExternalJdbcTableName(externalName, databaseMetaData);

            checkTableExists(externalTableName, databaseMetaData);
            Set<String> pkColumns = readPrimaryKeyColumns(externalTableName, databaseMetaData);
            return readColumns(externalTableName, databaseMetaData, pkColumns);
        } catch (Exception exception) {
            throw new HazelcastException("Could not execute readDbFields for table "
                                         + quoteCompoundIdentifier(externalName), exception);
        }
    }

    private static void checkTableExists(ExternalJdbcTableName externalTableName, DatabaseMetaData databaseMetaData)
            throws SQLException {
        String table = externalTableName.table;
        if (isMySQL(databaseMetaData)) {
            SqlDialect dialect = resolveDialect(databaseMetaData);
            //MySQL databaseMetaData.getTables requires quotes/backticks in case of fancy names (e.g. with dots)
            //To make it simple we wrap all table names
            table = dialect.quoteIdentifier(table);
        }

        Connection connection = databaseMetaData.getConnection();
        // If catalog and schema are not specified as external name, use the catalog and schema of the connection
        String catalog = (externalTableName.catalog != null) ? externalTableName.catalog : connection.getCatalog();
        String schema = (externalTableName.schema != null) ? externalTableName.schema : connection.getSchema();

        try (ResultSet tables = databaseMetaData.getTables(
                catalog,
                schema,
                table,
                new String[]{"TABLE", "VIEW"}
        )) {
            if (!tables.next()) {
                String fullTableName = quoteCompoundIdentifier(
                        externalTableName.catalog,
                        externalTableName.schema,
                        externalTableName.table
                );
                throw new HazelcastException("Could not find table " + fullTableName);
            }
        }
    }

    private static Set<String> readPrimaryKeyColumns(ExternalJdbcTableName externalTableName,
                                                     DatabaseMetaData databaseMetaData) {
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

    private static Map<String, DbField> readColumns(ExternalJdbcTableName externalTableName,
                                                    DatabaseMetaData databaseMetaData,
                                                    Set<String> pkColumns) {
        Map<String, DbField> fields = new LinkedHashMap<>();
        try (ResultSet resultSet = databaseMetaData.getColumns(
                externalTableName.catalog,
                externalTableName.schema,
                externalTableName.table,
                null)) {
            while (resultSet.next()) {
                String columnTypeName = resultSet.getString("TYPE_NAME");
                int precision = resultSet.getInt("COLUMN_SIZE");
                int scale = resultSet.getInt("DECIMAL_DIGITS");
                String columnName = resultSet.getString("COLUMN_NAME");
                fields.put(columnName,
                        new DbField(columnTypeName,
                                precision,
                                scale,
                                columnName,
                                pkColumns.contains(columnName)
                        ));
            }
        } catch (SQLException e) {
            throw new HazelcastException("Could not read columns for table " + externalTableName, e);
        }
        return fields;
    }

    private void validateType(TypeResolver typeResolver, MappingField field, DbField dbField) {
        QueryDataType type = typeResolver.resolveType(dbField.columnTypeName, dbField.precision, dbField.scale);
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
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> resolvedFields) {
        String dataConnectionName = externalResource.dataConnection();
        assert dataConnectionName != null;

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

        return new JdbcTable(
                this,
                fields,
                schemaName,
                mappingName,
                externalResource,
                new ConstantTableStatistics(0)
        );
    }

    static SqlDialect resolveDialect(JdbcTable table, DagBuildContext context) {
        String dataConnectionName = table.getDataConnectionName();
        JdbcDataConnection dataConnection = context
                .getNodeEngine()
                .getDataConnectionService()
                .getAndRetainDataConnection(dataConnectionName, JdbcDataConnection.class);

        try (Connection connection = dataConnection.getConnection()) {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            SupportedDatabases.logOnceIfDatabaseNotSupported(databaseMetaData);
            return resolveDialect(databaseMetaData);
        } catch (Exception e) {
            throw new HazelcastException("Could not determine dialect for data connection: " + dataConnectionName, e);
        } finally {
            dataConnection.release();
        }
    }

    private static SqlDialect resolveDialect(DatabaseMetaData databaseMetaData) throws SQLException {
        switch (DatabaseDialect.resolveDialect(databaseMetaData)) {
            case MYSQL:
                return new HazelcastMySqlDialect(SqlDialects.createContext(databaseMetaData));
            case MICROSOFT_SQL_SERVER:
                return new HazelcastMSSQLDialect(SqlDialects.createContext(databaseMetaData));
            case ORACLE:
                return new HazelcastOracleDialect(SqlDialects.createContext(databaseMetaData));
            default:
                return SqlDialectFactoryImpl.INSTANCE.create(databaseMetaData);
        }
    }

    @Nonnull
    @Override
    public VertexWithInputConfig nestedLoopReader(
            @Nonnull DagBuildContext context,
            @Nullable HazelcastRexNode predicate,
            @Nonnull List<HazelcastRexNode> projection,
            @Nonnull JetJoinInfo joinInfo) {

        JdbcTable jdbcTable = context.getTable();

        String namePrefix = "nestedLoopReader(" + jdbcTable.getExternalNameList() + ")";
        DAG dag = context.getDag();
        Vertex vertex = dag.newUniqueVertex(
                namePrefix,
                JdbcJoiner.createJoinProcessorSupplier(
                        joinInfo,
                        context, predicate, projection
                )
        );
        return new VertexWithInputConfig(vertex.localParallelism(1));
    }

    @Nonnull
    @Override
    public Vertex fullScanReader(
            @Nonnull DagBuildContext context,
            @Nullable HazelcastRexNode predicate,
            @Nonnull List<HazelcastRexNode> projection,
            @Nullable List<Map<String, Expression<?>>> partitionPruningCandidates,
            @Nullable FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider
    ) {
        if (eventTimePolicyProvider != null) {
            throw QueryException.error("Ordering functions are not supported on top of " + TYPE_NAME + " mappings");
        }
        JdbcTable table = context.getTable();
        SqlDialect dialect = resolveDialect(table, context);

        RexNode rexPredicate = predicate == null ? null : predicate.unwrap(RexNode.class);
        List<RexNode> rexProjection = Util.toList(projection, n -> n.unwrap(RexNode.class));

        SelectQueryBuilder builder = new SelectQueryBuilder(context.getTable(), dialect, rexPredicate, rexProjection);

        return context.getDag().newUniqueVertex(
                "Select(" + table.getExternalNameList() + ")",
                ProcessorMetaSupplier.forceTotalParallelismOne(
                        new SelectProcessorSupplier(
                                table.getDataConnectionName(),
                                builder.query(),
                                builder.parameterPositions(),
                                builder.converters()
                        ))
        );
    }

    @Nonnull
    @Override
    public VertexWithInputConfig insertProcessor(@Nonnull DagBuildContext context) {
        JdbcTable table = context.getTable();

        InsertQueryBuilder builder = new InsertQueryBuilder(table, resolveDialect(table, context));
        return new VertexWithInputConfig(context.getDag().newUniqueVertex(
                "Insert(" + table.getExternalNameList() + ")",
                new InsertProcessorSupplier(
                        table.getDataConnectionName(),
                        builder.query(),
                        table.getBatchLimit()
                )
        ).localParallelism(1));
    }

    @Nonnull
    @Override
    public List<String> getPrimaryKey(Table table0) {
        JdbcTable table = (JdbcTable) table0;
        return table.getPrimaryKeyList();
    }

    @Override
    public boolean supportsExpression(@Nonnull HazelcastRexNode expression) {
        RexNode rexExpression = expression.unwrap(RexNode.class);
        SupportsRexVisitor visitor = new SupportsRexVisitor();
        Boolean supports = rexExpression.accept(visitor);
        return supports != null && supports;
    }

    @Nonnull
    @Override
    public Vertex updateProcessor(
            @Nonnull DagBuildContext context,
            @Nonnull List<String> fieldNames,
            @Nonnull List<HazelcastRexNode> expressions,
            @Nullable HazelcastRexNode predicate,
            boolean hasInput
    ) {
        // We don't allow both predicate and input at the same time
        assert predicate == null || !hasInput;

        JdbcTable table = context.getTable();

        List<RexNode> rexExpressions = Util.toList(expressions, n -> n.unwrap(RexNode.class));
        RexNode rexPredicate = predicate == null ? null : predicate.unwrap(RexNode.class);

        UpdateQueryBuilder builder = new UpdateQueryBuilder(table,
                resolveDialect(table, context),
                fieldNames,
                rexExpressions,
                rexPredicate,
                hasInput
        );

        DmlProcessorSupplier updatePS = new DmlProcessorSupplier(
                table.getDataConnectionName(),
                builder.query(),
                builder.dynamicParams(),
                builder.inputRefs(),
                table.getBatchLimit()
        );

        return dmlVertex(context, hasInput, table, updatePS, "Update");
    }

    @Nonnull
    @Override
    public Vertex deleteProcessor(
            @Nonnull DagBuildContext context,
            @Nullable HazelcastRexNode predicate,
            boolean hasInput
    ) {
        // We don't allow both predicate and input at the same time
        assert predicate == null || !hasInput;

        JdbcTable table = context.getTable();

        RexNode rexPredicate = predicate == null ? null : predicate.unwrap(RexNode.class);

        DeleteQueryBuilder builder = new DeleteQueryBuilder(table,
                resolveDialect(table, context),
                rexPredicate,
                hasInput
        );

        DmlProcessorSupplier deletePS = new DmlProcessorSupplier(
                table.getDataConnectionName(),
                builder.query(),
                builder.dynamicParams(),
                builder.inputRefs(),
                table.getBatchLimit()
        );

        return dmlVertex(context, hasInput, table, deletePS, "Delete");
    }

    private static Vertex dmlVertex(
            DagBuildContext context,
            boolean hasInput,
            JdbcTable table,
            DmlProcessorSupplier processorSupplier,
            String statement
    ) {

        if (!hasInput) {
            // There is no input, we push the whole update/delete query to the database, but we need single dummy item
            // to execute the update/delete in WriteJdbcP
            // We can consider refactoring the WriteJdbcP, so it doesn't need the dummy item in the future.

            // Use local member address to run the dummy source and processor on the same member
            Address localAddress = context.getNodeEngine().getThisAddress();

            Vertex v = dummySourceVertex(context, "DummySourceFor" + statement, localAddress);
            Vertex dmlVertex = context.getDag().newUniqueVertex(
                    statement + "(" + table.getExternalNameList() + ")",
                    forceTotalParallelismOne(processorSupplier, localAddress)
            );

            context.getDag().edge(Edge.between(v, dmlVertex));
            return dmlVertex;
        } else {
            Vertex dmlVertex = context.getDag().newUniqueVertex(
                    statement + "(" + table.getExternalNameList() + ")",
                    processorSupplier
            ).localParallelism(1);
            return dmlVertex;
        }
    }

    private static Vertex dummySourceVertex(DagBuildContext context, String name, Address localAddress) {
        Vertex v = context.getDag().newUniqueVertex(name,
                forceTotalParallelismOne(
                        of(() -> new SingleItemSourceP<>(DUMMY_INPUT_ROW)),
                        localAddress
                )
        );
        return v;
    }

    @Nonnull
    @Override
    public Vertex sinkProcessor(@Nonnull DagBuildContext context) {
        JdbcTable jdbcTable = context.getTable();
        SqlDialect dialect = resolveDialect(jdbcTable, context);

        // If dialect is supported
        if (SupportedDatabases.isDialectSupported(dialect)) {
            // Get the upsert statement
            String upsertStatement = UpsertBuilder.getUpsertStatement(jdbcTable, dialect);

            // Create Vertex with the UPSERT statement
            return context.getDag().newUniqueVertex(
                    "sinkProcessor(" + jdbcTable.getExternalNameList() + ")",
                    new UpsertProcessorSupplier(
                            jdbcTable.getDataConnectionName(),
                            upsertStatement,
                            jdbcTable.getBatchLimit()
                    )
            ).localParallelism(1);
        }
        // Unsupported dialect. Create Vertex with the INSERT statement
        VertexWithInputConfig vertexWithInputConfig = insertProcessor(context);
        return vertexWithInputConfig.vertex();
    }

    private static boolean isMySQL(DatabaseMetaData databaseMetaData) throws SQLException {
        return getProductName(databaseMetaData).equals("MYSQL");
    }

    private static String getProductName(DatabaseMetaData databaseMetaData) throws SQLException {
        return databaseMetaData.getDatabaseProductName().toUpperCase(Locale.ROOT).trim();
    }

    private static class DbField {

        final String columnTypeName;
        final int precision;
        final int scale;
        final String columnName;
        final boolean primaryKey;

        DbField(String columnTypeName, int precision, int scale, String columnName, boolean primaryKey) {
            this.columnTypeName = requireNonNull(columnTypeName);
            this.precision = precision;
            this.scale = scale;
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

        ExternalJdbcTableName(String[] externalName, DatabaseMetaData databaseMetaData) throws SQLException {
            if (externalName.length == 1) {
                catalog = null;
                schema = null;
                table = externalName[0];
            } else if (externalName.length == 2) {
                if (isMySQL(databaseMetaData)) {
                    catalog = externalName[0];
                    schema = null;
                } else {
                    catalog = null;
                    schema = externalName[0];
                }
                table = externalName[1];
            } else if (externalName.length == 3) {
                if (isMySQL(databaseMetaData)) {
                    throw QueryException.error("Invalid external name " + quoteCompoundIdentifier(externalName)
                                               + ", external name for MySQL must have either 1 or 2 components "
                                               + "(catalog and relation)");
                }
                catalog = externalName[0];
                schema = externalName[1];
                table = externalName[2];
            } else {
                // external name length was validated earlier, we should never get here
                throw QueryException.error("Invalid external name length");
            }
        }

        static void validateExternalName(String[] externalName) {
            // External name must have at least 1 and at most 3 components
            if (externalName.length == 0 || externalName.length > 3) {
                throw QueryException.error("Invalid external name " + quoteCompoundIdentifier(externalName)
                                           + ", external name for Jdbc must have either 1, 2 or 3 components "
                                           + "(catalog, schema and relation)");
            }
        }
    }

    public static BiFunctionEx<ResultSet, Integer, ?>[] prepareValueGettersFromMetadata(
            TypeResolver typeResolver,
            ResultSet rs,
            Function<Integer, FunctionEx<? super Object, ?>> converterFn) throws SQLException {

        ResultSetMetaData metaData = rs.getMetaData();

        BiFunctionEx<ResultSet, Integer, Object>[] valueGetters = new BiFunctionEx[metaData.getColumnCount()];
        for (int j = 0; j < metaData.getColumnCount(); j++) {
            String type = metaData.getColumnTypeName(j + 1).toUpperCase(Locale.ROOT);
            int precision = metaData.getPrecision(j + 1);
            int scale = metaData.getScale(j + 1);
            QueryDataType resolvedType = typeResolver.resolveType(type, precision, scale);

            valueGetters[j] = GETTERS.getOrDefault(
                    resolvedType,
                    (resultSet, n) -> rs.getObject(n)
            ).andThen(converterFn.apply(j));
        }
        return valueGetters;
    }

}
