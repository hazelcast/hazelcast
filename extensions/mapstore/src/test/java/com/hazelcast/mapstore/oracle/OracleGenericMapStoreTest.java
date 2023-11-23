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

package com.hazelcast.mapstore.oracle;

import com.hazelcast.mapstore.GenericMapStoreTest;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.jdbc.OracleDatabaseProvider;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.sql.SQLException;

@Category(NightlyTest.class)
public class OracleGenericMapStoreTest extends GenericMapStoreTest {

    @BeforeClass
    public static void beforeClass() {
        initialize(new OracleDatabaseProvider());
    }

    @Override
    protected void createMapLoaderTable(String tableName) throws SQLException {
        createTable(tableName, "id NUMBER(8) PRIMARY KEY", "name VARCHAR(100)");
    }

    @Override
    protected void createMapLoaderTable(String tableName, String... columns) throws SQLException {
        for (int i = 0; i < columns.length; i++) {
            columns[i] = columns[i].replace("INT", "NUMBER(8)");
        }
        createTable(tableName, columns);
    }
}
