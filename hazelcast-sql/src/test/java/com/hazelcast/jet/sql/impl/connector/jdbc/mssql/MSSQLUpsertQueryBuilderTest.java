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

package com.hazelcast.jet.sql.impl.connector.jdbc.mssql;

import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcTable;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.MssqlSqlDialect;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class MSSQLUpsertQueryBuilderTest {

    @Mock
    JdbcTable jdbcTable;

    SqlDialect dialect = MssqlSqlDialect.DEFAULT;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        when(jdbcTable.getExternalName()).thenReturn(new String[]{"table1"});
        when(jdbcTable.getExternalNameList()).thenReturn(singletonList("table1"));
        when(jdbcTable.dbFieldNames()).thenReturn(Arrays.asList("field1", "field2"));
        when(jdbcTable.getPrimaryKeyList()).thenReturn(Arrays.asList("pk1", "pk2"));
    }

    @Test
    public void appendIFClause() {
        MSSQLUpsertQueryBuilder builder = new MSSQLUpsertQueryBuilder(jdbcTable, dialect);
        StringBuilder sb = new StringBuilder();
        builder.appendIFClause(sb);
        String insertClause = sb.toString();
        assertThat(insertClause).isEqualTo("IF NOT EXISTS (SELECT 1 FROM [table1] WHERE [pk1]=? AND [pk2]=?) BEGIN INSERT INTO [table1] ([field1],[field2])");
    }

    @Test
    public void appendValuesClause() {
        MSSQLUpsertQueryBuilder builder = new MSSQLUpsertQueryBuilder(jdbcTable, dialect);
        StringBuilder sb = new StringBuilder();
        builder.appendValuesClause(sb);

        String valuesClause = sb.toString();
        assertThat(valuesClause).isEqualTo("VALUES (?,?) END");
    }

    @Test
    public void appendElseClause() {
        MSSQLUpsertQueryBuilder builder = new MSSQLUpsertQueryBuilder(jdbcTable, dialect);
        StringBuilder sb = new StringBuilder();
        builder.appendElseClause(sb);

        String valuesClause = sb.toString();
        assertThat(valuesClause).isEqualTo(
                "ELSE BEGIN UPDATE [table1] SET [field1] = ? ,[field2] = ?"
                        + "  WHERE [pk1]=? AND [pk2]=? END");
    }

    @Test
    public void query() {
        MSSQLUpsertQueryBuilder builder = new MSSQLUpsertQueryBuilder(jdbcTable, dialect);
        String result = builder.query();
        assertThat(result).isEqualTo(
                "IF NOT EXISTS (SELECT 1 FROM [table1] WHERE [pk1]=? AND [pk2]=?) BEGIN INSERT INTO [table1] ([field1],[field2])"
                        + " VALUES (?,?) END ELSE BEGIN UPDATE [table1] SET [field1] = ? ,[field2] = ?  WHERE [pk1]=? AND [pk2]=? END"
        );
    }
}

