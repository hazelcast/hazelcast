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

import com.hazelcast.jet.sql.impl.connector.jdbc.SchemaJdbcConnectorTest;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.jdbc.MySQLDatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import static org.junit.Assume.assumeFalse;

@Category(NightlyTest.class)
public class MySQLSchemaJdbcSqlConnectorTest extends SchemaJdbcConnectorTest {

    @BeforeClass
    public static void beforeClass() {
        initialize(new MySQLDatabaseProvider());
    }

    @Before
    public void checkMySQLVersion() throws Exception {
        // We suffer from the following issue when useInformationSchema is false
        // https://bugs.mysql.com/bug.php?id=63992
        // This value is set to true on MySQL 8.0.3+
        // https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-metadata.html
        // Therefore we ignore the `table.with.dot` case for MySQL 5

        assumeFalse(MySQLDatabaseProvider.TEST_MYSQL_VERSION.startsWith("5")
                && table.equals("table.with.dot"));
    }

}
