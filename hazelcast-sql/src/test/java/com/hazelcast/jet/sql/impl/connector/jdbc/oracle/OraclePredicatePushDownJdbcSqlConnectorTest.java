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

import com.hazelcast.jet.sql.impl.connector.jdbc.PredicatePushDownJdbcSqlConnectorTest;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.jdbc.OracleDatabaseProvider;
import com.hazelcast.test.jdbc.TestDatabaseProvider;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.sql.SQLException;

@Category(NightlyTest.class)
public class OraclePredicatePushDownJdbcSqlConnectorTest extends PredicatePushDownJdbcSqlConnectorTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        initializePredicatePushDownTestOracle(new OracleDatabaseProvider());
    }

    private static void initializePredicatePushDownTestOracle(TestDatabaseProvider provider) throws SQLException {
        initialize(provider);

        tableName = "\"people\"";

        createTable(tableName,
                "\"id\" INT PRIMARY KEY",
                "\"name\" VARCHAR(100)",
                "\"age\" INT",
                "\"data\" VARCHAR(100)",
                "\"a\" INT", "\"b\" INT", "\"c\" INT", "\"d\" INT",
                "\"nullable_column\" VARCHAR(100)",
                "\"nullable_column_reverse\" VARCHAR(100)"
        );

        executeJdbc("INSERT INTO " + tableName + " VALUES (1, 'John Doe', 30, '{\"value\":42}', 1, 1, 0, " +
                "1, null, 'not null reverse')");
        executeJdbc("INSERT INTO " + tableName + " VALUES (2, 'Jane Doe', 35, '{\"value\":0}', 0, 0, 1, " +
                "1, 'not null', null)");
    }
}
