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

package com.hazelcast.jet.sql.impl.connector.jdbc.oracle;

import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcTable;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Arrays;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OracleUpsertQueryBuilderTest {

    @Mock
    JdbcTable jdbcTable;

    SqlDialect dialect = OracleSqlDialect.DEFAULT;

    @Before
    public void setUp() {
        jdbcTable = mock(JdbcTable.class);
        when(jdbcTable.getExternalName()).thenReturn(new String[]{"table1"});
        when(jdbcTable.getExternalNameList()).thenReturn(singletonList("table1"));
        when(jdbcTable.dbFieldNames()).thenReturn(Arrays.asList("field1", "field2"));
        when(jdbcTable.getPrimaryKeyList()).thenReturn(Arrays.asList("pk1", "pk2"));
    }

    @Test
    public void appendMergeClause() {
        OracleUpsertQueryBuilder builder = new OracleUpsertQueryBuilder(jdbcTable, dialect);
        StringBuilder sb = new StringBuilder();
        builder.appendMergeClause(sb);
        String mergeClause = sb.toString();
        assertThat(mergeClause).isEqualTo(
                "MERGE INTO \"table1\" TARGET "
                        + "USING (SELECT ? as \"field1\", ? as \"field2\" FROM dual) "
                        + "SOURCE ON (TARGET.\"pk1\" = SOURCE.\"pk1\" AND TARGET.\"pk2\" = SOURCE.\"pk2\")"
        );
    }

    @Test
    public void appendMatchedClause() {
        OracleUpsertQueryBuilder builder = new OracleUpsertQueryBuilder(jdbcTable, dialect);
        StringBuilder sb = new StringBuilder();
        builder.appendMatchedClause(sb);

        String matchedClause = sb.toString();
        assertThat(matchedClause).isEqualTo(
                "WHEN MATCHED THEN "
                        + "UPDATE SET "
                        + "TARGET.\"field1\" = SOURCE.\"field1\", "
                        + "TARGET.\"field2\" = SOURCE.\"field2\" "
                        + "WHEN NOT MATCHED THEN "
                        + "INSERT (\"field1\", \"field2\") VALUES(SOURCE.\"field1\", SOURCE.\"field2\")"
        );
    }

    @Test
    public void query() {
        OracleUpsertQueryBuilder builder = new OracleUpsertQueryBuilder(jdbcTable, dialect);
        String result = builder.query();
        assertThat(result).isEqualTo(
                "MERGE INTO \"table1\" TARGET "
                        + "USING (SELECT ? as \"field1\", ? as \"field2\" FROM dual) "
                        + "SOURCE ON (TARGET.\"pk1\" = SOURCE.\"pk1\" AND TARGET.\"pk2\" = SOURCE.\"pk2\") "
                        + "WHEN MATCHED THEN "
                        + "UPDATE SET "
                        + "TARGET.\"field1\" = SOURCE.\"field1\", "
                        + "TARGET.\"field2\" = SOURCE.\"field2\" "
                        + "WHEN NOT MATCHED THEN "
                        + "INSERT (\"field1\", \"field2\") VALUES(SOURCE.\"field1\", SOURCE.\"field2\")"
        );
    }
}

