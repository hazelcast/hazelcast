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

package com.hazelcast.jet.sql.impl.connector.jdbc.postgres;

import com.hazelcast.jet.sql.impl.connector.jdbc.MappingJdbcSqlConnectorTest;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.jdbc.PostgresDatabaseProvider;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatCode;

@Category(NightlyTest.class)
public class PostgresMappingJdbcSqlConnectorTest extends MappingJdbcSqlConnectorTest {

    @BeforeClass
    public static void beforeClass() {
        initialize(new PostgresDatabaseProvider());
    }

    // Text is not a standard SQL type. This test specific to PostgresSQL
    @Test
    public void createMappingWithTextColumnType() throws Exception {
        executeJdbc("CREATE TABLE " + tableName + " (id INTEGER NOT NULL,name TEXT NOT NULL)");
        insertItems(tableName, 1);

        assertThatCode(() ->
                execute("CREATE MAPPING postgresMapping "
                        + " EXTERNAL NAME  " + tableName
                        + " ("
                        + "id INTEGER,"
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
                )
        ).doesNotThrowAnyException();

        assertRowsAnyOrder(
                "SELECT * FROM postgresMapping",
                Collections.singletonList(new Row(0, "name-0"))
        );
    }

}
