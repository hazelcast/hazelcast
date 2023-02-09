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

package com.hazelcast.jet.sql.impl.connector.jdbc.h2;

import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcTable;
import org.apache.calcite.sql.SqlDialect;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class H2UpsertQueryBuilderTest {
    @Mock
    JdbcTable jdbcTable;

    @Mock
    SqlDialect sqlDialect;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        when(jdbcTable.getExternalName()).thenReturn("table1");
        when(jdbcTable.getPrimaryKeyList()).thenReturn(Arrays.asList("pk1", "pk2"));
        when(jdbcTable.dbFieldNames()).thenReturn(Arrays.asList("field1", "field2"));
        when(jdbcTable.sqlDialect()).thenReturn(sqlDialect);

        when(sqlDialect.quoteIdentifier(anyString())).thenAnswer((InvocationOnMock invocation) -> {
            Object argument = invocation.getArguments()[0];
            return "\"" + argument + "\"";
        });
    }

    @Test
    public void testGetMergeClause() {
        H2UpsertQueryBuilder builder = new H2UpsertQueryBuilder(jdbcTable);
        StringBuilder stringBuilder = new StringBuilder();
        builder.getMergeClause(stringBuilder);

        String insertClause = stringBuilder.toString();
        assertEquals("MERGE INTO \"table1\" (\"field1\",\"field2\") ", insertClause);
    }

    @Test
    public void testGetKeyClause() {
        when(jdbcTable.getExternalName()).thenReturn("table1");
        when(jdbcTable.getPrimaryKeyList()).thenReturn(Arrays.asList("pk1", "pk2"));
        when(jdbcTable.dbFieldNames()).thenReturn(Arrays.asList("field1", "field2"));

        H2UpsertQueryBuilder builder = new H2UpsertQueryBuilder(jdbcTable);
        StringBuilder stringBuilder = new StringBuilder();
        builder.getKeyClause(stringBuilder);

        String keyClause = stringBuilder.toString();
        assertEquals("KEY (\"pk1\",\"pk2\") ", keyClause);
    }

    @Test
    public void testGetValuesClause() {
        H2UpsertQueryBuilder builder = new H2UpsertQueryBuilder(jdbcTable);
        StringBuilder stringBuilder = new StringBuilder();
        builder.getValuesClause(stringBuilder);

        String valueClause = stringBuilder.toString();
        assertEquals("VALUES (?,?)", valueClause);
    }

    @Test
    public void testQuery() {
        H2UpsertQueryBuilder builder = new H2UpsertQueryBuilder(jdbcTable);

        String query = builder.query();
        assertEquals("MERGE INTO \"table1\" (\"field1\",\"field2\") KEY (\"pk1\",\"pk2\") VALUES (?,?)", query);
    }
}
