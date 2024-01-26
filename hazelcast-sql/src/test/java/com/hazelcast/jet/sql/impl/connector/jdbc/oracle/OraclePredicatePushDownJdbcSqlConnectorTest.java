/*
 * Copyright 2024 Hazelcast Inc.
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
import org.junit.Before;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assumptions.assumeThat;

@Category(NightlyTest.class)
public class OraclePredicatePushDownJdbcSqlConnectorTest extends PredicatePushDownJdbcSqlConnectorTest {

    @Before
    @Override
    public void setUp() throws Exception {
        assumeThat(query)
                .describedAs("Oracle doesn't support 'IS NOT TRUE'/'IS NOT FALSE' predicates")
                .doesNotContain("IS NOT TRUE", "IS NOT FALSE");

        super.setUp();
    }
}
