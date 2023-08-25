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

import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlConnectorStabilityTest;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.jdbc.OracleDatabaseProvider;
import com.hazelcast.test.jdbc.TestDatabaseProvider;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.util.Locale;

@Category(NightlyTest.class)
public class OracleJdbcSqlConnectorStabilityTest extends JdbcSqlConnectorStabilityTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        initializeStabilityTestOracle(new OracleDatabaseProvider());
    }

    private static void initializeStabilityTestOracle(TestDatabaseProvider provider) throws Exception {
        initialize(provider);

        tableName = randomTableName().toUpperCase(Locale.ROOT);
        executeJdbc("CREATE TABLE " + tableName + " (ID INT PRIMARY KEY, " + "NAME VARCHAR(100))");
        insertItems(tableName, 5);

        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " ID INT, "
                        + " NAME VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        databaseProvider.shutdown();
    }
}
