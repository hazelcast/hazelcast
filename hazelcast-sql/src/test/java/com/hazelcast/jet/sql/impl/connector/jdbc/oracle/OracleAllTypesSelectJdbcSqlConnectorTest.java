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

import com.hazelcast.jet.sql.impl.connector.jdbc.AllTypesSelectJdbcSqlConnectorTest;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.jdbc.OracleDatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.util.Locale;

import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.util.Lists.newArrayList;

@Category(NightlyTest.class)
public class OracleAllTypesSelectJdbcSqlConnectorTest extends AllTypesSelectJdbcSqlConnectorTest {

    @BeforeClass
    public static void beforeClass() {
        initialize(new OracleDatabaseProvider());
    }

    @Before
    public void setUp() throws Exception {
        assumeThat(type).describedAs("TINYINT not supported on Oracle")
                .isNotEqualTo("TINYINT");

        assumeThat(type).describedAs("BOOLEAN not supported on Oracle")
                .isNotEqualTo("BOOLEAN");

        assumeThat(type).describedAs("BIGINT not supported on Oracle")
                .isNotEqualTo("BIGINT");

        assumeThat(type).describedAs("TIME not supported on Oracle")
                .isNotEqualTo("TIME");


        if (type.equals("DOUBLE")) {
            type = "DOUBLE PRECISION";
        }
        if (type.equals("DATE")) {
            value = "DATE'2022-12-30'";
        }
        if (type.equals("TIMESTAMP")) {
            value = "TIMESTAMP'2022-12-30 23:59:59'";
        }
        if (type.equals("TIMESTAMP WITH TIME ZONE")) {
            value = "TIMESTAMP'2022-12-30 23:59:59 -05:00'";
        }
    }

}
