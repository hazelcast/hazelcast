/*
 * Copyright 2025 Hazelcast Inc.
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

import com.hazelcast.jet.sql.impl.connector.jdbc.AllTypesInsertJdbcSqlConnectorTest;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.jdbc.MySQLDatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assumptions.assumeThat;

@Category(NightlyTest.class)
public class MySQLAllTypesInsertJdbcSqlConnectorTest extends AllTypesInsertJdbcSqlConnectorTest {

    @Parameterized.Parameters(name = "type:{0}, mappingType:{1}, sqlValue:{2}, javaValue:{3}, jdbcValue:{4}")
    public static Collection<Object[]> parameters() {
        // Include parameters from the parent class
        Collection<Object[]> parentParams = AllTypesInsertJdbcSqlConnectorTest.parameters();

        // Add additional parameters in the child class
        List<Object[]> list = new ArrayList<>(parentParams);

        Object[][] additionalData = {
                {"TINYTEXT", "VARCHAR", "'dummy'", "dummy", "dummy"},
                {"MEDIUMTEXT", "VARCHAR", "'dummy'", "dummy", "dummy"},
                {"LONGTEXT", "VARCHAR", "'dummy'", "dummy", "dummy"},
                {"TINYINT UNSIGNED", "SMALLINT", "255", (short) 255, (short) 255},
                {"SMALLINT UNSIGNED", "INTEGER", "65535", 65535, 65535},
                {"MEDIUMINT", "INTEGER", "-8388608", -8388608, -8388608},
                {"MEDIUMINT UNSIGNED", "INTEGER", "16777215", 16777215, 16777215},
                {"YEAR", "INTEGER", "2024", 2024, 2024},
                {"INT UNSIGNED", "BIGINT", "4294967295", 4294967295L, 4294967295L},
                {"BIGINT UNSIGNED", "DECIMAL", "18446744073709551615", new BigDecimal("18446744073709551615"),
                        new BigDecimal("18446744073709551615")},
        };
        list.addAll(asList(additionalData));

        return list;
    }

    @BeforeClass
    public static void beforeClass() {
        initialize(new MySQLDatabaseProvider());
    }

    @Before
    public void setUp() throws Exception {
        assumeThat(type).describedAs("TIMESTAMP WITH TIME ZONE not supported on MySQL")
                .isNotEqualTo("TIMESTAMP WITH TIME ZONE");

        // MySQL REAL type is by default a synonym for DOUBLE PRECISION
        // MySQL uses FLOAT as 4-byte floating point type
        if (type.equals("REAL")) {
            type = "FLOAT";
        }
    }

}
