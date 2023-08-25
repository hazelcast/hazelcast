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

package com.hazelcast.jet.sql.impl.connector.jdbc.mysql;

import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcTable;
import com.hazelcast.mock.MockUtil;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Arrays;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class MySQLUpsertQueryBuilderTest {

    @Mock
    JdbcTable jdbcTable;

    SqlDialect dialect = MysqlSqlDialect.DEFAULT;

    private AutoCloseable openMocks;

    @Before
    public void setUp() {
        openMocks = openMocks(this);

        when(jdbcTable.getExternalName()).thenReturn(new String[]{"table1"});
        when(jdbcTable.getExternalNameList()).thenReturn(singletonList("table1"));
        when(jdbcTable.dbFieldNames()).thenReturn(Arrays.asList("field1", "field2"));
    }

    @After
    public void cleanUp() {
        MockUtil.closeMocks(openMocks);
    }

    @Test
    public void appendInsertClause() {
        MySQLUpsertQueryBuilder builder = new MySQLUpsertQueryBuilder(jdbcTable, dialect);
        StringBuilder sb = new StringBuilder();
        builder.appendInsertClause(sb);

        String insertClause = sb.toString();
        assertThat(insertClause).isEqualTo("INSERT INTO `table1` (`field1`,`field2`)");
    }

    @Test
    public void appendValuesClause() {
        MySQLUpsertQueryBuilder builder = new MySQLUpsertQueryBuilder(jdbcTable, dialect);
        StringBuilder sb = new StringBuilder();
        builder.appendValuesClause(sb);

        String valuesClause = sb.toString();
        assertThat(valuesClause).isEqualTo("VALUES (?,?)");
    }

    @Test
    public void appendOnDuplicateClause() {
        MySQLUpsertQueryBuilder builder = new MySQLUpsertQueryBuilder(jdbcTable, dialect);
        StringBuilder sb = new StringBuilder();
        builder.appendOnDuplicateClause(sb);

        String valuesClause = sb.toString();
        assertThat(valuesClause).isEqualTo(
                "ON DUPLICATE KEY UPDATE " +
                        "`field1` = VALUES(`field1`)," +
                        "`field2` = VALUES(`field2`)");
    }

    @Test
    public void query() {
        MySQLUpsertQueryBuilder builder = new MySQLUpsertQueryBuilder(jdbcTable, dialect);
        String result = builder.query();
        assertThat(result).isEqualTo(
                "INSERT INTO `table1` (`field1`,`field2`) VALUES (?,?)" +
                        " ON DUPLICATE KEY UPDATE `field1` = VALUES(`field1`),`field2` = VALUES(`field2`)"
        );
    }
}
