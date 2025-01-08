/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.jet.cdc;

import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@SuppressWarnings("SqlResolve")
public final class PostgresTestUtils {
    private PostgresTestUtils() {
    }


    public static Connection getPostgreSqlConnection(String url, String user, String password) throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    static void insertData(PostgreSQLContainer<?> container) {
        try (Connection connection = getPostgreSqlConnection(container.getJdbcUrl(), container.getUsername(),
                container.getPassword())) {
            connection.setSchema("inventory");
            try (Statement statement = connection.createStatement()) {
                statement.addBatch("UPDATE customers SET first_name='Anne Marie' WHERE id=1004");
                statement.addBatch("INSERT INTO customers VALUES (1005, 'Jason', 'Bourne', 'jason@bourne.org')");
                statement.addBatch("DELETE FROM customers WHERE id=1005");
                statement.executeBatch();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


}
