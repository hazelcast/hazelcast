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

import org.junit.BeforeClass;
import org.junit.Test;

import java.net.ConnectException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class DbUnreachableJdbcSqlConnectorTest extends JdbcSqlTestSupport {

    @BeforeClass
    public static void beforeClass() {
        initialize(2, smallInstanceConfig());
        sqlService = instance().getSql();
    }

    @Test
    public void createMappingShouldThrowCorrectExceptionWhenDatabaseNotAvailable() {
        sqlService.execute("CREATE DATA CONNECTION mysql "
                + "TYPE JDBC "
                + "OPTIONS("
                + "'jdbcUrl'='jdbc:mysql:/fake-host:12345/noSuchDb', "
                + "'username'='root', "
                + "'password'='123'"
                + ")");

        assertThatThrownBy(() ->
                sqlService.execute("CREATE MAPPING people DATA CONNECTION mysql")
        ).hasStackTraceContaining("Could not create pool for data connection 'mysql'")
         .hasRootCauseInstanceOf(ConnectException.class);
    }
}
