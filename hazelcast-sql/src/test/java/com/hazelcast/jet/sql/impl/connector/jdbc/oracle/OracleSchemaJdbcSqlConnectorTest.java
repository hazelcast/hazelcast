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

import com.hazelcast.jet.sql.impl.connector.jdbc.SchemaJdbcConnectorTest;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.jdbc.OracleDatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assumptions.assumeThat;


@Category(NightlyTest.class)
public class OracleSchemaJdbcSqlConnectorTest extends SchemaJdbcConnectorTest {

    @BeforeClass
    public static void beforeClass() {
        initialize(new OracleDatabaseProvider());
    }

    @Before
    public void setUp() throws Exception {
        assumeThat(schema).describedAs("Name with double quotes not supported on Oracle")
                .isNotEqualTo("schema_with_quote\"");
        tableFull = quote(schema, table);
        try {
            executeJdbc(databaseProvider.createSchemaQuery(quote(schema)));
            executeJdbc("GRANT UNLIMITED TABLESPACE TO " + quote(schema));
        } catch (Exception e) {
            logger.info("Could not create schema", e);
        }
        createTable(tableFull);
    }
}
