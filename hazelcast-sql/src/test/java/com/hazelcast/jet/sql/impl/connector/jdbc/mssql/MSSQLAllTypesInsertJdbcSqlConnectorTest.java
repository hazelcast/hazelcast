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

package com.hazelcast.jet.sql.impl.connector.jdbc.mssql;

import com.hazelcast.jet.sql.impl.connector.jdbc.AllTypesInsertJdbcSqlConnectorTest;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.jdbc.MSSQLDatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assumptions.assumeThat;


@Category(NightlyTest.class)
public class MSSQLAllTypesInsertJdbcSqlConnectorTest extends AllTypesInsertJdbcSqlConnectorTest {

    @Parameterized.Parameters(name = "type:{0}, mappingType:{1}, sqlValue:{2}, javaValue:{3}, jdbcValue:{4}")
    public static Collection<Object[]> parameters() {
        // Include parameters from the parent class
        Collection<Object[]> parentParams = AllTypesInsertJdbcSqlConnectorTest.parameters();

        // Add additional parameters in the child class
        List<Object[]> list = new ArrayList<>(parentParams);

        Object[][] additionalData = {
                {"DATETIME2", "TIMESTAMP", "'2022-12-30 23:59:59'",
                        LocalDateTime.of(2022, 12, 30, 23, 59, 59),
                        Timestamp.valueOf("2022-12-30 23:59:59")},

                // Accuracy of SMALLDATETIME is 1 minute. Round to nearest minute
                {"SMALLDATETIME", "TIMESTAMP", "'2022-12-30 23:59:59'",
                        LocalDateTime.of(2022, 12, 31, 0, 0, 0),
                        Timestamp.valueOf("2022-12-31 00:00:00")},
        };
        list.addAll(asList(additionalData));

        return list;
    }

    @BeforeClass
    public static void beforeClass() {
        initialize(new MSSQLDatabaseProvider());
    }

    @Before
    public void setUp() throws Exception {
        // MSSQL doesn't support BOOLEAN
        assumeThat(type).describedAs("BOOLEAN not supported on MSSQL")
                .isNotEqualTo("BOOLEAN");

        // MSSQL uses FLOAT as 8-byte floating point type
        // For Float in MSSQL see https://learn.microsoft.com/en-us/sql/t-sql/data-types/float-and-real-transact-sql?view=sql-server-ver16
        if (type.equals("DOUBLE")) {
            type = "FLOAT";
        }

        // For TIMESTAMP in MSSQL see https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2005/ms182776(v=sql.90)?redirectedfrom=MSDN
        if (type.equals("TIMESTAMP")) {
            type = "DATETIME";
        }

        // For DATETIMEOFFSET in MSSQL see https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetimeoffset-transact-sql?view=sql-server-ver16
        if (type.equals("TIMESTAMP WITH TIME ZONE")) {
            type = "DATETIMEOFFSET";
        }
    }

}
