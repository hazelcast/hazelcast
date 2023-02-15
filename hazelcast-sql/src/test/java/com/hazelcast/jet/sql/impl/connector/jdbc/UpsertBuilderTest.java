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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class UpsertBuilderTest {

    @Mock
    JdbcTable jdbcTable;

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

        boolean result = UpsertBuilder.isUpsertDialectSupported(jdbcTable);
        assertFalse(result);
    }

    @Test
    public void testUpsertDialectSupported() {

        when(jdbcTable.sqlDialect()).thenReturn(mysqlSqlDialect);

        boolean result = UpsertBuilder.isUpsertDialectSupported(jdbcTable);
        assertTrue(result);
    }

}
