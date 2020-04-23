/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.jdbc;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;


/**
 * JDBC database metadata.
 */
// TODO: Remove NP_NONNULL_RETURN_VIOLATION suppression when ready
@SuppressWarnings({"RedundantThrows", "checkstyle:MethodCount"})
@SuppressFBWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "Prototype")
public class HazelcastJdbcDatabaseMetadata implements DatabaseMetaData {
    /** Connection which produced this metadata. */
    private final HazelcastJdbcConnection connection;

    public HazelcastJdbcDatabaseMetadata(HazelcastJdbcConnection connection) {
        this.connection = connection;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public String getURL() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public String getUserName() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public String getDriverName() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public String getDriverVersion() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public int getDriverMajorVersion() {
        // TODO
        return 0;
    }

    @Override
    public int getDriverMinorVersion() {
        // TODO
        return 0;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public String getStringFunctions() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        // TODO

        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return level == Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
        String columnNamePattern) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
        throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
        String columnNamePattern) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
        throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
        throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog,
        String foreignSchema, String foreignTable) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
        throws SQLException {
        // TODO
        return null;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
        throws SQLException {
        // TODO
        return null;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
        String columnNamePattern) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
        throws SQLException {
        // TODO
        return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // TODO
        return false;
    }
}
