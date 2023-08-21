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

import com.hazelcast.jet.sql.impl.connector.jdbc.AllTypesInsertJdbcSqlConnectorTest;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.jdbc.OracleDatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.util.Locale;

import static org.assertj.core.api.Assumptions.assumeThat;

@Category(NightlyTest.class)
public class OracleAllTypesInsertJdbcSqlConnectorTest extends AllTypesInsertJdbcSqlConnectorTest {

    @BeforeClass
    public static void beforeClass() {
        initialize(new OracleDatabaseProvider());
    }

    @Override
    protected String generateTableName(){
        return randomTableName().toUpperCase(Locale.ROOT);
    }

    @Override
    protected void createTable_(String tableName) throws Exception{
        createTable(tableName, "ID INT", "TABLE_COLUMN " + type);
    }

    @Override
    protected void executeMapping(String mappingName, String tableName){
        execute("CREATE MAPPING " + mappingName
                + " EXTERNAL NAME " + tableName
                + " ("
                + "ID INT, "
                + "TABLE_COLUMN " + mappingType
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );
    }
    /*
    @Before
    public void setUp() throws Exception {
        assumeThat(type).describedAs("TINYINT not supported on Postgres")
                .isNotEqualTo("TINYINT");

        assumeThat(type).describedAs("TIMESTAMP WITH TIME ZONE not supported on Postgres")
                .isNotEqualTo("TIMESTAMP WITH TIME ZONE");


        if (type.equals("DOUBLE")) {
            type = "DOUBLE PRECISION";
        }
    }
    */
}

