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

import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.SybaseSqlDialect;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class SupportedDatabasesTest {

    @Mock
    JdbcTable jdbcTable;

    // Create mock dialects for JdbcTable
    @Mock
    SybaseSqlDialect sybaseSqlDialect;

    @Mock
    MysqlSqlDialect mysqlSqlDialect;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testUpsertDialectNotSupported() {
        when(jdbcTable.sqlDialect()).thenReturn(sybaseSqlDialect);

        boolean result = SupportedDatabases.isDialectSupported(jdbcTable);
        assertFalse(result);
    }

    @Test
    public void testUpsertDialectSupported() {
        when(jdbcTable.sqlDialect()).thenReturn(mysqlSqlDialect);

        boolean result = SupportedDatabases.isDialectSupported(jdbcTable);
        assertTrue(result);
    }

    @Test
    public void testMySQL() {
        String dbName = "MYSQL";
        boolean newDB1 = SupportedDatabases.isNewDatabase(dbName);
        assertThat(newDB1).isFalse();
    }

    @Test
    public void testPostgreSQL() {
        String dbName = "POSTGRESQL";
        boolean newDB = SupportedDatabases.isNewDatabase(dbName);
        assertThat(newDB).isFalse();
    }

    @Test
    public void testH2() {
        String dbName = "H2";
        boolean newDB = SupportedDatabases.isNewDatabase(dbName);
        assertThat(newDB).isFalse();
    }

    @Test
    public void test_notSupportedDB() {
        String dbName = "cassandra";
        boolean newDB = SupportedDatabases.isNewDatabase(dbName);
        assertThat(newDB).isTrue();
    }
}
