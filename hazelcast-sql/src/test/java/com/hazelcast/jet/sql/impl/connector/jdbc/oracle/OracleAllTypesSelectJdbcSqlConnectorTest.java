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

import com.hazelcast.jet.sql.impl.connector.jdbc.AllTypesSelectJdbcSqlConnectorTest;
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
public class OracleAllTypesSelectJdbcSqlConnectorTest extends AllTypesSelectJdbcSqlConnectorTest {

    @Parameterized.Parameters(name = "type:{0}, mappingType:{1}, value:{2}, expected:{3}")
    public static Collection<Object[]> parameters() {
        // Include parameters from the parent class
        Collection<Object[]> parentParams = AllTypesSelectJdbcSqlConnectorTest.parameters();

        // Add additional parameters in the child class
        List<Object[]> list = new ArrayList<>(parentParams);

        Object[][] additionalData = {
                {"BINARY_FLOAT", "REAL", "1.5", 1.5f},
                {"BINARY_DOUBLE", "DOUBLE", "1.8", 1.8},
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

        // If Oracle Free is true, the test will always run regardless of the type
        // If Oracle Free is false, the test will be ignored when the type is "BOOLEAN"
        assumeThat(OracleDatabaseProviderFactory.isOracleFreeVersion() || !type.equals("BOOLEAN"))
                .describedAs("BOOLEAN not supported on Oracle")
                .isTrue();

        assumeThat(type).describedAs("TIME not supported on Oracle")
                .isNotEqualTo("TIME");



        if (type.equals("DATE")) {
            value = "DATE'2022-12-30'";
        }
        if (type.equals("TIMESTAMP")) {
            value = "TIMESTAMP'2022-12-30 23:59:59'";
        }
        if (type.equals("TIMESTAMP WITH TIME ZONE")) {
            value = "TIMESTAMP'2022-12-30 23:59:59 -05:00'";
        }
        if (type.equals("SMALLINT")) {
            type = "NUMBER(4)";
        }
        if (type.equals("INTEGER")) {
            type = "NUMBER(5)";
        }
        if (type.equals("BIGINT")) {
            type = "NUMBER(18)";
        }
        if (type.equals("REAL")) {
            type = "NUMBER(6,1)";
        }
        if (type.equals("DOUBLE")) {
            type = "NUMBER(14,1)";
        }
        if (type.equals("DECIMAL (10,5)")) {
            type = "DECIMAL (11,5)";
            //this still works, decimal will be converted to NUMBER with the same precision and scale
            //Changed from precision 10 to 11 because it falls into double's interval
        }
    }

}
