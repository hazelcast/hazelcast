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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.config.Config;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.test.IgnoreInJenkinsOnWindows;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.jdbc.TestDatabaseProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.IntStream;

import static com.hazelcast.dataconnection.impl.DataConnectionTestUtil.configureJdbcDataConnection;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertOrdered;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

@Category({QuickTest.class, IgnoreInJenkinsOnWindows.class})
public abstract class ReadJdbcPPropertiesTest extends SimpleTestInClusterSupport {

    protected static TestDatabaseProvider databaseProvider;

    private static final int ITEM_COUNT = 100;
    private static final String JDBC_DATA_CONNECTION = "jdbc-data-connection";

    private static String dbConnectionUrl;
    private static List<Entry<Integer, String>> tableContents;

    @BeforeClass
    public static void beforeClassCheckDocker() {
        assumeDockerEnabled();
    }

    protected static void initializeBeforeClass(TestDatabaseProvider testDatabaseProvider, String... args) throws SQLException {
        databaseProvider = testDatabaseProvider;
        dbConnectionUrl = databaseProvider.createDatabase(ReadJdbcPPropertiesTest.class.getName());
        dbConnectionUrl = dbConnectionUrl + String.join("", args);


        Config config = smallInstanceConfig();
        configureJdbcDataConnection(JDBC_DATA_CONNECTION, dbConnectionUrl, config);
        initialize(2, config);
        // create and fill a table
        try (Connection conn = DriverManager.getConnection(dbConnectionUrl);
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute("CREATE TABLE items(id INT PRIMARY KEY, name VARCHAR(10))");
            for (int i = 0; i < ITEM_COUNT; i++) {
                stmt.execute(String.format("INSERT INTO items VALUES(%d, 'name-%d')", i, i));
            }
        }
        tableContents = IntStream.range(0, ITEM_COUNT).mapToObj(i -> entry(i, "name-" + i)).collect(toList());
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (databaseProvider != null) {
            databaseProvider.shutdown();
            databaseProvider = null;
            dbConnectionUrl = null;
        }
    }


    protected void runTestFetchSize(Properties properties, int fetchSize) {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.jdbc(dbConnectionUrl, "select * from items",
                        properties,
                        resultSet -> {
                            assertThat(resultSet.getFetchSize()).isEqualTo(fetchSize);
                            return entry(resultSet.getInt(1), resultSet.getString(2));
                        }
                ))
                .writeTo(assertOrdered(tableContents));

        instance().getJet().newJob(p).join();
    }

    protected void runTest(Properties properties) {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.jdbc(dbConnectionUrl, "select * from items",
                        properties,
                        resultSet -> entry(resultSet.getInt(1), resultSet.getString(2))
                ))
                .writeTo(assertOrdered(tableContents));

        instance().getJet().newJob(p).join();
    }
}
