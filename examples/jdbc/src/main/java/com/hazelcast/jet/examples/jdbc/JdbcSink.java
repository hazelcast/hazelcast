/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.examples.jdbc;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import org.h2.tools.DeleteDbFiles;

import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * Demonstrates dumping values from an IMap to a table in a relational database
 * using the JDBC connector.
 */
public class JdbcSink {

    private static final String MAP_NAME = "userMap";
    private static final String TABLE_NAME = "USER_TABLE";

    private JetInstance jet;
    private String dbDirectory;

    private static Pipeline buildPipeline(String connectionUrl) {
        Pipeline p = Pipeline.create();

        p.readFrom(Sources.<Integer, User>map(MAP_NAME))
         .map(Map.Entry::getValue)
         .writeTo(Sinks.jdbc("INSERT INTO " + TABLE_NAME + "(id, name) VALUES(?, ?)",
                 connectionUrl,
                 (stmt, user) -> {
                     // Bind the values from the stream item to a PreparedStatement created from
                     // the above query.
                     stmt.setInt(1, user.getId());
                     stmt.setString(2, user.getName());
                 }));
        return p;
    }

    public static void main(String[] args) throws Exception {
        new JdbcSink().go();
    }

    private void go() throws Exception {
        try {
            setup();
            Pipeline p = buildPipeline(connectionUrl());
            jet.newJob(p).join();
            printTable();
        } finally {
            cleanup();
        }
    }

    private void setup() throws Exception {
        dbDirectory = Files.createTempDirectory(JdbcSink.class.getName()).toString();

        createTable();

        jet = Jet.bootstrappedInstance();

        IMap<Integer, User> map = jet.getMap(MAP_NAME);
        // populate the source IMap
        for (int i = 0; i < 100; i++) {
            map.put(i, new User(i, "name-" + i));
        }
    }

    private void cleanup() {
        Jet.shutdownAll();
        DeleteDbFiles.execute(dbDirectory, JdbcSink.class.getSimpleName(), true);
    }

    private void createTable() throws SQLException {
        try (
                Connection connection = DriverManager.getConnection(connectionUrl());
                Statement statement = connection.createStatement()
        ) {
            statement.execute("CREATE TABLE " + TABLE_NAME + "(id int primary key, name varchar(255))");
        }
    }

    private void printTable() throws SQLException {
        try (
                Connection connection = DriverManager.getConnection(connectionUrl());
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT * FROM " + TABLE_NAME)
        ) {
            while (resultSet.next()) {
                System.out.println(new User(resultSet.getInt(1), resultSet.getString(2)));
            }
        }
    }

    private String connectionUrl() {
        return "jdbc:h2:" + dbDirectory + "/" + JdbcSink.class.getSimpleName();
    }
}
