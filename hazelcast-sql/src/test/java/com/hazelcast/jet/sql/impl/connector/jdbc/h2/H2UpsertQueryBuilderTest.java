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
import org.apache.calcite.sql.dialect.H2SqlDialect;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class H2UpsertQueryBuilderTest {

    @Mock
    JdbcTable table;

    SqlDialect sqlDialect = H2SqlDialect.DEFAULT;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        when(table.getExternalName()).thenReturn(new String[]{"table1"});
        when(table.getExternalNameList()).thenReturn(Collections.singletonList("table1"));
        when(table.getPrimaryKeyList()).thenReturn(Arrays.asList("pk1", "pk2"));
        when(table.dbFieldNames()).thenReturn(Arrays.asList("field1", "field2"));
        when(table.sqlDialect()).thenReturn(sqlDialect);
    }

    @Test
    public void appendMergeClause() {
        H2UpsertQueryBuilder builder = new H2UpsertQueryBuilder(table);
        StringBuilder sb = new StringBuilder();
        builder.appendMergeClause(sb);

        String mergeClause = sb.toString();
        assertThat(mergeClause).isEqualTo("MERGE INTO \"table1\" (\"field1\",\"field2\")");
    }

    @Test
    public void appendKeyClause() {
        H2UpsertQueryBuilder builder = new H2UpsertQueryBuilder(table);
        StringBuilder sb = new StringBuilder();
        builder.appendKeyClause(sb);

        String keyClause = sb.toString();
        assertThat(keyClause).isEqualTo("KEY (\"pk1\",\"pk2\")");
    }

    @Test
    public void appendValuesClause() {
        H2UpsertQueryBuilder builder = new H2UpsertQueryBuilder(table);
        StringBuilder sb = new StringBuilder();
        builder.appendValuesClause(sb);

        String valueClause = sb.toString();
        assertThat(valueClause).isEqualTo("VALUES (?,?)");
    }

    @Test
    public void testQuery() {
        H2UpsertQueryBuilder builder = new H2UpsertQueryBuilder(table);

        String query = builder.query();
        assertThat(query).isEqualTo("MERGE INTO \"table1\" (\"field1\",\"field2\") KEY (\"pk1\",\"pk2\") VALUES (?,?)");
    }
}
