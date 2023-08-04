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

import com.hazelcast.jet.sql.impl.connector.jdbc.AllTypesSelectJdbcSqlConnectorTest;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.jdbc.MSSQLDatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;


@Category(NightlyTest.class)
public class MSSQLAllTypesSelectJdbcSqlConnectorTest extends AllTypesSelectJdbcSqlConnectorTest {


    @BeforeClass
    public static void beforeClass() {
        initialize(new MSSQLDatabaseProvider());
    }

    @Before
    public void setUp() throws Exception {
        // MSSQL doesn't support BOOLEAN but BIT can be used instead and, uses FLOAT as 8-byte floating point type
        // For Float in MSSQL see https://learn.microsoft.com/en-us/sql/t-sql/data-types/float-and-real-transact-sql?view=sql-server-ver16
        // For BIT in MSSQL see https://learn.microsoft.com/en-us/sql/t-sql/data-types/bit-transact-sql?view=sql-server-ver16
        // For TIMESTAMP in MSSQL see https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2005/ms182776(v=sql.90)?redirectedfrom=MSDN
        // For DATETIMEOFFSET in MSSQL see https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetimeoffset-transact-sql?view=sql-server-ver16

        if (type.equals("BOOLEAN")) {
            type = "BIT";
            value = "1"; //BIT cannot be true
        }
        if (type.equals("DOUBLE")) {
            type = "FLOAT";
        }
        if (type.equals("TIMESTAMP")) {
            type = "DATETIME";
        }
        if (type.equals("TIMESTAMP WITH TIME ZONE")) {
            type = "DATETIMEOFFSET";
        }
    }
}
