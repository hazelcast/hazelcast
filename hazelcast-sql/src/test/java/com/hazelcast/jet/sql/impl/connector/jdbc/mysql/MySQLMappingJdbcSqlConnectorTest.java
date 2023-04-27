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

import com.hazelcast.jet.sql.impl.connector.jdbc.MappingJdbcSqlConnectorTest;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.jdbc.MySQLDatabaseProvider;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Category(NightlyTest.class)
public class MySQLMappingJdbcSqlConnectorTest extends MappingJdbcSqlConnectorTest {

    @BeforeClass
    public static void beforeClass() {
        initialize(new MySQLDatabaseProvider());
    }

    @Test
    public void createMappingFails_invalid_externalNameFullName() throws SQLException {
        createTable(tableName);

        // Create invalid mapping
        assertThatThrownBy(() -> execute(
                "CREATE MAPPING " + tableName + " EXTERNAL NAME " + "foo.public.bar" + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        )).isInstanceOf(HazelcastSqlException.class)
          .hasRootCauseInstanceOf(QueryException.class)
          .hasStackTraceContaining("Invalid external name");
    }
}
