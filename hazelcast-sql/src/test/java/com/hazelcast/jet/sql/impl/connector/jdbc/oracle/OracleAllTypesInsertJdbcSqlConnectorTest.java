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

import com.hazelcast.jet.sql.impl.connector.jdbc.AllTypesInsertJdbcSqlConnectorTest;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.jdbc.OracleDatabaseProviderFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assumptions.assumeThat;

@Category(NightlyTest.class)
public class OracleAllTypesInsertJdbcSqlConnectorTest extends AllTypesInsertJdbcSqlConnectorTest {

    @Parameterized.Parameters(name = "type:{0}, mappingType:{1}, sqlValue:{2}, javaValue:{3}, jdbcValue:{4}")
    public static Collection<Object[]> parameters() {
        // Include parameters from the parent class
        Collection<Object[]> parentParams = AllTypesInsertJdbcSqlConnectorTest.parameters();

        // Add additional parameters in the child class
        List<Object[]> list = new ArrayList<>(parentParams);

        Object[][] additionalData = {
                {"BINARY_FLOAT", "REAL", "1.5", 1.5f, 1.5f},
                {"BINARY_DOUBLE", "DOUBLE", "1.8", 1.8, 1.8},
        };
        list.addAll(asList(additionalData));

        return list;
    }

    @BeforeClass
    public static void beforeClass() {
        initialize(OracleDatabaseProviderFactory.createTestDatabaseProvider());
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
    }

}

