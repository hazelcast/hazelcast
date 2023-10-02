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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MSSQLUpsertQueryBuilderTest {

    @Mock
    JdbcTable jdbcTable;

    SqlDialect dialect = MssqlSqlDialect.DEFAULT;

    @BeforeEach
    public void setUp() {
        when(jdbcTable.getExternalNameList()).thenReturn(singletonList("table1"));
        when(jdbcTable.dbFieldNames()).thenReturn(Arrays.asList("field1", "field2"));
        when(jdbcTable.getPrimaryKeyList()).thenReturn(Arrays.asList("pk1", "pk2"));
    }


    @Test
    void appendMergeClause() {
        MSSQLUpsertQueryBuilder builder = new MSSQLUpsertQueryBuilder(jdbcTable, dialect);
        StringBuilder sb = new StringBuilder();
        builder.appendMergeClause(sb);
        String mergeClause = sb.toString();
        assertThat(mergeClause).isEqualTo("MERGE [table1] USING (VALUES (?, ?)) AS source ([field1], [field2]) ON [table1].[pk1] = source.[pk1] AND [table1].[pk2] = source.[pk2]");
    }

    @Test
    void appendValuesClause() {
        MSSQLUpsertQueryBuilder builder = new MSSQLUpsertQueryBuilder(jdbcTable, dialect);
        StringBuilder sb = new StringBuilder();
        builder.appendValuesClause(sb);

        String valuesClause = sb.toString();
        assertThat(valuesClause).isEqualTo("VALUES (?, ?)");
    }

    @Test
    void appendMatchedClause() {
        MSSQLUpsertQueryBuilder builder = new MSSQLUpsertQueryBuilder(jdbcTable, dialect);
        StringBuilder sb = new StringBuilder();
        builder.appendMatchedClause(sb);

        String matchedClause = sb.toString();
        assertThat(matchedClause).isEqualTo(
                "WHEN MATCHED THEN UPDATE SET [field1] = source.[field1], [field2] = source.[field2] "
                        + "WHEN NOT MATCHED THEN INSERT ([field1], [field2]) VALUES(source.[field1], source.[field2]);");
    }

    @Test
    void query() {
        MSSQLUpsertQueryBuilder builder = new MSSQLUpsertQueryBuilder(jdbcTable, dialect);
        String result = builder.query();
        assertThat(result).isEqualTo(
                "MERGE [table1] USING (VALUES (?, ?)) AS source ([field1], [field2]) ON [table1].[pk1] = source.[pk1] AND [table1].[pk2] = source.[pk2] "
                        + "WHEN MATCHED THEN UPDATE SET [field1] = source.[field1], [field2] = source.[field2] "
                        + "WHEN NOT MATCHED THEN INSERT ([field1], [field2]) VALUES(source.[field1], source.[field2]);"
        );
    }
}

