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

package com.hazelcast.jet.sql.impl.connector.jdbc.mssql;

import com.hazelcast.jet.sql.impl.connector.jdbc.PredicatePushDownJdbcSqlConnectorTest;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.jdbc.MSSQLDatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assumptions.assumeThat;

@Category(NightlyTest.class)
public class MSSQLPredicatePushDownJdbcSqlConnectorTest extends PredicatePushDownJdbcSqlConnectorTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        initializePredicatePushDownTest(new MSSQLDatabaseProvider());
    }

    @Before
    public void checkOperatorsSupported() throws Exception {
        assumeThat(query)
                .describedAs("'IS' and 'IS NOT' operators are not supported in MS SQL")
                .doesNotContain("IS TRUE", "IS FALSE", "IS NOT TRUE", "IS NOT FALSE");
    }
}
