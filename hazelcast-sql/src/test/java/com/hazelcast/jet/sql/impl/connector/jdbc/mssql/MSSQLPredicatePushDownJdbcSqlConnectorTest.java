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

import java.sql.SQLException;

@Category(NightlyTest.class)
public class MSSQLPredicatePushDownJdbcSqlConnectorTest extends PredicatePushDownJdbcSqlConnectorTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        initializePredicatePushDownTestMSSQL(new MSSQLDatabaseProvider());
    }

    @Before
    public void setUpForMSSQL() throws Exception {
        //These statements have syntax incompatible with MSSQL
        if(query.equals("SELECT name FROM people WHERE a AND b")){
            query = "SELECT name FROM people WHERE a = '1' AND b = '1'";
        }else if(query.equals("SELECT name FROM people WHERE a OR b")){
            query = "SELECT name FROM people WHERE a = '1' OR b = '1'";
        }else if(query.equals("SELECT name FROM people WHERE NOT c")){
            query = "SELECT name FROM people WHERE c != '1'";
        }else if(query.equals("SELECT name FROM people WHERE a IS TRUE")){
            query = "SELECT name FROM people WHERE a = '1'";
        }else if(query.equals("SELECT name FROM people WHERE c IS FALSE")){
            query = "SELECT name FROM people WHERE c = '0'";
        }else if(query.equals("SELECT name FROM people WHERE c IS NOT TRUE")){
            query = "SELECT name FROM people WHERE c != '1'";
        }else if(query.equals("SELECT name FROM people WHERE a IS NOT FALSE")){
            query = "SELECT name FROM people WHERE a != '0'";
        }else if(query.equals("SELECT name FROM people WHERE LENGTH(data) = 12")){
            query = "SELECT name FROM people WHERE LEN(data) = 12";
        }

    }

    private static void initializePredicatePushDownTestMSSQL(MSSQLDatabaseProvider databaseProvider) throws SQLException {
        initialize(databaseProvider);

        tableName = "people";

        createTable(tableName,
                "id INT PRIMARY KEY",
                "name VARCHAR(100)",
                "age INT",
                "data VARCHAR(100)",
                "a VARCHAR(100)", "b VARCHAR(100)", "c VARCHAR(100)", "d VARCHAR(100)",
                "nullable_column VARCHAR(100)",
                "nullable_column_reverse VARCHAR(100)"
        );

        executeJdbc("INSERT INTO " + tableName + " VALUES (1, 'John Doe', 30, '{\"value\":42}', 1, 1, 0, " +
                "1, null, 'not null reverse')");
        executeJdbc("INSERT INTO " + tableName + " VALUES (2, 'Jane Doe', 35, '{\"value\":0}', 0, 0, 1, " +
                "1, 'not null', null)");
    }
}
