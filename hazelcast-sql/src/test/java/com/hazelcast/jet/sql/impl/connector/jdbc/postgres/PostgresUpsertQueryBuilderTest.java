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

package com.hazelcast.jet.sql.impl.connector.jdbc.postgres;

import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcTable;
import com.hazelcast.mock.MockUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PostgresUpsertQueryBuilderTest {

    @Mock
    JdbcTable jdbcTable;

    SqlDialect dialect = PostgresqlSqlDialect.DEFAULT;

    private AutoCloseable openMocks;

    @Before
    public void setUp() {
        openMocks = openMocks(this);

        when(jdbcTable.getExternalName()).thenReturn(new String[]{"table1"});
        when(jdbcTable.getExternalNameList()).thenReturn(Collections.singletonList("table1"));
        when(jdbcTable.getPrimaryKeyList()).thenReturn(Arrays.asList("pk1", "pk2"));
        when(jdbcTable.dbFieldNames()).thenReturn(Arrays.asList("field1", "field2"));
    }

    @After
    public void cleanUp() {
        MockUtil.closeMocks(openMocks);
    }

    @Test
    public void testAppendInsertClause() {
        PostgresUpsertQueryBuilder builder = new PostgresUpsertQueryBuilder(jdbcTable, dialect);
        StringBuilder sb = new StringBuilder();
        builder.appendInsertClause(sb);

        String insertClause = sb.toString();
        assertThat(insertClause).isEqualTo("INSERT INTO \"table1\" (\"field1\",\"field2\")");
    }

    @Test
    public void testAppendValuesClause() {
        PostgresUpsertQueryBuilder builder = new PostgresUpsertQueryBuilder(jdbcTable, dialect);
        StringBuilder sb = new StringBuilder();
        builder.appendValuesClause(sb);

        String valuesClause = sb.toString();
        assertThat(valuesClause).isEqualTo("VALUES (?,?)");
    }

    @Test
    public void testAppendOnConflictClause() {
        PostgresUpsertQueryBuilder builder = new PostgresUpsertQueryBuilder(jdbcTable, dialect);
        StringBuilder sb = new StringBuilder();
        builder.appendOnConflictClause(sb);

        String valuesClause = sb.toString();
        assertThat(valuesClause).isEqualTo(
                "ON CONFLICT (\"pk1\",\"pk2\") " +
                        "DO UPDATE SET " +
                        "\"field1\" = EXCLUDED.\"field1\"," +
                        "\"field2\" = EXCLUDED.\"field2\""
        );
    }

    @Test
    public void testQuery() {
        PostgresUpsertQueryBuilder builder = new PostgresUpsertQueryBuilder(jdbcTable, dialect);
        String result = builder.query();
        String expected = "INSERT INTO \"table1\" (\"field1\",\"field2\") VALUES (?,?) ON CONFLICT (\"pk1\",\"pk2\") " +
                "DO UPDATE SET \"field1\" = EXCLUDED.\"field1\",\"field2\" = EXCLUDED.\"field2\"";
        assertThat(result).isEqualTo(expected);
    }
}
