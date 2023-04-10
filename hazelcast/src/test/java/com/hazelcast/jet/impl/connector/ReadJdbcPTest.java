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
import com.hazelcast.dataconnection.impl.JdbcDataConnection;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.dataconnection.impl.DataConnectionTestUtil.configureDummyDataConnection;
import static com.hazelcast.dataconnection.impl.DataConnectionTestUtil.configureJdbcDataConnection;
import static com.hazelcast.jet.pipeline.DataConnectionRef.dataConnectionRef;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertAnyOrder;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertOrdered;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Category({QuickTest.class, ParallelJVMTest.class})
public class ReadJdbcPTest extends SimpleTestInClusterSupport {

    private static final int ITEM_COUNT = 100;
    private static final String JDBC_DATA_CONNECTION = "jdbc-data-connection";
    private static final String DUMMY_DATA_CONNECTION = "dummy-data-connection";

    private static String dbConnectionUrl;
    private static List<Entry<Integer, String>> tableContents;

    @BeforeClass
    public static void setupClass() throws SQLException {
        dbConnectionUrl = "jdbc:h2:mem:" + ReadJdbcPTest.class.getSimpleName() + ";DB_CLOSE_DELAY=-1";

        Config config = smallInstanceConfig();
        configureJdbcDataConnection(JDBC_DATA_CONNECTION, dbConnectionUrl, config);
        configureDummyDataConnection(DUMMY_DATA_CONNECTION, config);
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
        try (Connection conn = DriverManager.getConnection(dbConnectionUrl)) {
            conn.createStatement().execute("shutdown");
        }
    }

    @Test
    public void test_whenPartitionedQuery() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.jdbc(
                () -> DriverManager.getConnection(dbConnectionUrl),
                (con, parallelism, index) -> {
                    PreparedStatement statement = con.prepareStatement("select * from items where mod(id,?)=?");
                    statement.setInt(1, parallelism);
                    statement.setInt(2, index);
                    return statement.executeQuery();
                },
                resultSet -> entry(resultSet.getInt(1), resultSet.getString(2))))
         .writeTo(assertAnyOrder(tableContents));

        instance().getJet().newJob(p).join();
    }

    @Test
    public void should_work_with_dataConnection() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.jdbc(
                        dataConnectionRef(JDBC_DATA_CONNECTION),
                        (con, parallelism, index) -> {
                            PreparedStatement statement = con.prepareStatement("select * from items where mod(id,?)=?");
                            statement.setInt(1, parallelism);
                            statement.setInt(2, index);
                            return statement.executeQuery();
                        },
                        resultSet -> entry(resultSet.getInt(1), resultSet.getString(2))))
                .writeTo(assertAnyOrder(tableContents));

        instance().getJet().newJob(p).join();
    }

    @Test
    public void should_fail_with_non_existing_dataConnection() {

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.jdbc(
                        dataConnectionRef("non-existing-data-connection"),
                        (con, parallelism, index) -> {
                            PreparedStatement statement = con.prepareStatement("select * from items where mod(id,?)=?");
                            statement.setInt(1, parallelism);
                            statement.setInt(2, index);
                            return statement.executeQuery();
                        },
                        resultSet -> entry(resultSet.getInt(1), resultSet.getString(2))))
                .writeTo(assertAnyOrder(tableContents));

        assertThatThrownBy(() -> instance().getJet().newJob(p).join())
                .hasMessageContaining("Data connection 'non-existing-data-connection' not found");
    }

    @Test
    public void should_fail_with_non_jdbc_dataConnection() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.jdbc(
                        dataConnectionRef(DUMMY_DATA_CONNECTION),
                        (con, parallelism, index) -> {
                            PreparedStatement statement = con.prepareStatement("select * from items where mod(id,?)=?");
                            statement.setInt(1, parallelism);
                            statement.setInt(2, index);
                            return statement.executeQuery();
                        },
                        resultSet -> entry(resultSet.getInt(1), resultSet.getString(2))))
                .writeTo(assertAnyOrder(tableContents));

        assertThatThrownBy(() -> instance().getJet().newJob(p).join())
                .hasMessageContaining("Data connection '" + DUMMY_DATA_CONNECTION
                        + "' must be an instance of class " + JdbcDataConnection.class.getName());
    }

    @Test
    public void test_whenTotalParallelismOne() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.jdbc(dbConnectionUrl, "select * from items",
                resultSet -> entry(resultSet.getInt(1), resultSet.getString(2))))
         .writeTo(assertOrdered(tableContents));

        instance().getJet().newJob(p).join();
    }

}
