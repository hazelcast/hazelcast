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

package com.hazelcast.jet.sql.impl.connector.jdbc.mysql;

import com.hazelcast.jet.sql.impl.connector.jdbc.AllTypesSelectJdbcSqlConnectorTest;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.jdbc.MySQLDatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assumptions.assumeThat;

@Category({NightlyTest.class, ParallelJVMTest.class})
public class MySQLAllTypesSelectJdbcSqlConnectorTest extends AllTypesSelectJdbcSqlConnectorTest {

    /*
     * Use same name (`beforeClass`) as in JdbcSqlTestSupport to hide the method and call it after
     * we set the correct database provider. If we used different name the method in parent would run first,
     * initializing the default H2 database.
     */
    @BeforeClass
    public static void beforeClass() {
        initialize(new MySQLDatabaseProvider());
    }

    @Before
    public void setUp() throws Exception {
        assumeThat(type).isNotEqualTo("TIMESTAMP WITH TIME ZONE")
                .describedAs("TIMESTAMP WITH TIME ZONE not supported on MySQL");
    }
}
