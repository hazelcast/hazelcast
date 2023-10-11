/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.dataconnection.impl;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.dataconnection.DataConnectionResource;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import com.hazelcast.test.jdbc.MSSQLDatabaseProvider;
import com.hazelcast.test.jdbc.MySQLDatabaseProvider;
import com.hazelcast.test.jdbc.PostgresDatabaseProvider;
import com.hazelcast.test.jdbc.TestDatabaseProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static com.hazelcast.dataconnection.impl.JdbcDataConnection.OBJECT_TYPE_TABLE;
import static com.hazelcast.test.DockerTestUtil.assumeTestDatabaseProviderCanLaunch;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ListResourcesTest {

    @Parameters(name = "provider == {0}")
    public static Object[] params() {
        return new Object[][]{
                {
                        new H2DatabaseProvider(),
                        new DataConnectionResource[]{
                                new DataConnectionResource(OBJECT_TYPE_TABLE, "testdb", "PUBLIC", "my_table"),
                                new DataConnectionResource(OBJECT_TYPE_TABLE, "testdb", "my_schema", "my_table")
                        }
                },
                {
                        new PostgresDatabaseProvider(),
                        new DataConnectionResource[]{
                                new DataConnectionResource(OBJECT_TYPE_TABLE, "public", "my_table"),
                                new DataConnectionResource(OBJECT_TYPE_TABLE, "my_schema", "my_table")
                        }
                },
                {
                        new MySQLDatabaseProvider(),
                        new DataConnectionResource[]{
                                new DataConnectionResource(OBJECT_TYPE_TABLE, "testdb", "my_table"),
                                new DataConnectionResource(OBJECT_TYPE_TABLE, "my_schema", "my_table")
                        }
                },
                {
                        new MSSQLDatabaseProvider(),
                        new DataConnectionResource[]{
                                new DataConnectionResource(OBJECT_TYPE_TABLE, "master", "dbo", "my_table"),
                                new DataConnectionResource(OBJECT_TYPE_TABLE, "master", "my_schema", "my_table")
                        }
                },
        };
    }

    @Parameter
    public TestDatabaseProvider provider;

    @Parameter(1)
    public DataConnectionResource[] expectedResources;

    @Before
    public void setUp() {
        // Currently, Windows can not launch some providers
        assumeTestDatabaseProviderCanLaunch(provider);
    }

    @Test
    public void shouldListResourcesDefaultAndNonDefaultSchema() throws Exception {
        String jdbcUrl = provider.createDatabase("testdb");

        JdbcDataConnection dataConnection = new JdbcDataConnection(new DataConnectionConfig()
                .setType("jdbc")
                .setProperty("jdbcUrl", jdbcUrl)
        );

        executeJdbc(jdbcUrl, "CREATE TABLE my_table (id INTEGER, name VARCHAR(255) )");
        executeJdbc(jdbcUrl, "CREATE SCHEMA my_schema");
        executeJdbc(jdbcUrl, "CREATE TABLE my_schema.my_table (id INTEGER, name VARCHAR(255) )");

        List<DataConnectionResource> dataConnectionResources = dataConnection.listResources();
        assertThat(dataConnectionResources).containsExactlyInAnyOrder(expectedResources);
    }

    public static void executeJdbc(String url, String sql) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute(sql);
        }
    }
}
