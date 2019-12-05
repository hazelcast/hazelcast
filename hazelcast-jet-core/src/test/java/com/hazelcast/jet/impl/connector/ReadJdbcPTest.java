/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.pipeline.PipelineTestSupport;
import com.hazelcast.jet.pipeline.Sources;
import org.h2.tools.DeleteDbFiles;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

public class ReadJdbcPTest extends PipelineTestSupport {

    private static final int PERSON_COUNT = 100;

    private static Path tempDirectory;
    private static String dbConnectionUrl;

    @BeforeClass
    public static void setupClass() throws SQLException, IOException {
        String dbName = ReadJdbcPTest.class.getSimpleName();
        tempDirectory = Files.createTempDirectory(dbName);
        dbConnectionUrl = "jdbc:h2:" + tempDirectory + "/" + dbName;

        createAndFillTable();
    }

    @AfterClass
    public static void deleteDbFiles() throws IOException {
        DeleteDbFiles.execute(tempDirectory.toString(), ReadJdbcPTest.class.getSimpleName(), true);
        Files.delete(tempDirectory);
    }

    @Test
    public void test_whenPartitionedQuery() {
        p.readFrom(Sources.jdbc(
                () -> DriverManager.getConnection(dbConnectionUrl),
                (con, parallelism, index) -> {
                    PreparedStatement statement = con.prepareStatement("select * from PERSON where mod(id,?)=?");
                    statement.setInt(1, parallelism);
                    statement.setInt(2, index);
                    return statement.executeQuery();
                },
                resultSet -> new Person(resultSet.getInt(1), resultSet.getString(2))))
         .writeTo(sink);

        execute();

        assertEquals(PERSON_COUNT, sinkList.size());
    }

    @Test
    public void test_whenTotalParallelismOne() {
        p.readFrom(Sources.jdbc(dbConnectionUrl, "select * from PERSON",
                resultSet -> new Person(resultSet.getInt(1), resultSet.getString(2))))
         .writeTo(sink);

        execute();

        assertEquals(PERSON_COUNT, sinkList.size());
    }

    private static void createAndFillTable() throws SQLException {
        try (Connection connection = DriverManager.getConnection(dbConnectionUrl);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE PERSON(id int primary key, name varchar(255))");
            try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO PERSON(id, name) VALUES(?, ?)")) {
                for (int i = 1; i <= PERSON_COUNT; i++) {
                    stmt.setInt(1, i);
                    stmt.setString(2, "name-" + i);
                    stmt.executeUpdate();
                }
            }
        }
    }

    private static final class Person implements Serializable {

        private final int id;
        private final String name;

        private Person(int id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    '}';
        }
    }
}
