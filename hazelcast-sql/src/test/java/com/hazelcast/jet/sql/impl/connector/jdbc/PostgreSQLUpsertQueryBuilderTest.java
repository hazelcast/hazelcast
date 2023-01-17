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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PostgreSQLUpsertQueryBuilderTest {

    @Mock
    JdbcTable jdbcTable;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGetInsertClause() {
        when(jdbcTable.getExternalName()).thenReturn("table1");
        when(jdbcTable.dbFieldNames()).thenReturn(Arrays.asList("field1", "field2"));

        PostgreSQLUpsertQueryBuilder builder = new PostgreSQLUpsertQueryBuilder(jdbcTable);
        StringBuilder stringBuilder = new StringBuilder();
        builder.getInsertClause(jdbcTable, stringBuilder);

        String insertClause = stringBuilder.toString();
        assertEquals("INSERT INTO table1 (field1,field2) ", insertClause);
    }

    @Test
    public void testGetValuesClause() {
        when(jdbcTable.dbFieldNames()).thenReturn(Arrays.asList("field1", "field2"));

        PostgreSQLUpsertQueryBuilder builder = new PostgreSQLUpsertQueryBuilder(jdbcTable);
        StringBuilder stringBuilder = new StringBuilder();
        builder.getValuesClause(jdbcTable, stringBuilder);

        String valuesClause = stringBuilder.toString();
        assertEquals("VALUES (?,?) ", valuesClause);
    }

    @Test
    public void testGetOnConflictClause() {
        when(jdbcTable.getPrimaryKeyList()).thenReturn(Arrays.asList("pk1", "pk2"));
        when(jdbcTable.dbFieldNames()).thenReturn(Arrays.asList("field1", "field2"));

        PostgreSQLUpsertQueryBuilder builder = new PostgreSQLUpsertQueryBuilder(jdbcTable);
        StringBuilder stringBuilder = new StringBuilder();
        builder.getOnConflictClause(jdbcTable, stringBuilder);

        String valuesClause = stringBuilder.toString();
        assertEquals("ON CONFLICT (pk1,pk2) DO UPDATE SET field1 = EXCLUDED.field1,field2 = EXCLUDED.field2", valuesClause);
    }

    @Test
    public void testQuery() {
        when(jdbcTable.getExternalName()).thenReturn("table1");
        when(jdbcTable.getPrimaryKeyList()).thenReturn(Arrays.asList("pk1", "pk2"));
        when(jdbcTable.dbFieldNames()).thenReturn(Arrays.asList("field1", "field2"));

        PostgreSQLUpsertQueryBuilder builder = new PostgreSQLUpsertQueryBuilder(jdbcTable);
        String result = builder.query();
        String expected = "INSERT INTO table1 (field1,field2) VALUES (?,?) ON CONFLICT (pk1,pk2) DO UPDATE SET " +
                          "field1 = EXCLUDED.field1,field2 = EXCLUDED.field2";
        assertEquals(expected, result);
    }
}
