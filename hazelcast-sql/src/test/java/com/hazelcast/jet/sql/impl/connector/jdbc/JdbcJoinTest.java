/*
 * Copyright 2021 Hazelcast Inc.
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

import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlConnector.OPTION_EXTERNAL_DATASTORE_REF;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JdbcJoinTest extends JdbcSqlTestSupport {

    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider());
    }

    @Test
    public void test_stream2BatchJoinAsNestedLoopJoinIsNotSupported() throws Exception {
        String tableName = randomTableName();
        createTable(tableName);
        insertItems(tableName, 5);

        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ( "
                        + " '" + OPTION_EXTERNAL_DATASTORE_REF + "'='" + TEST_DATABASE_REF + "'"
                        + ")"
        );

        assertThatThrownBy(() ->
                sqlService.execute("SELECT n.name, t.v FROM " +
                        "TABLE(GENERATE_STREAM(2)) t " +
                        "JOIN " + tableName + " n ON n.id = t.v;")
        ).hasMessageContaining("JDBC connector doesn't support stream-to-batch JOIN");
    }
}
