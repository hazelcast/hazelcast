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

package com.hazelcast.jet.sql.impl.connector.jdbc;

import com.hazelcast.test.jdbc.TestDatabaseProvider;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseRule implements TestRule {

    private TestDatabaseProvider databaseProvider;
    private String dbConnectionUrl;

    public String getDbConnectionUrl() {
        return dbConnectionUrl;
    }

    public DatabaseRule(TestDatabaseProvider databaseProvider) {
        this.databaseProvider = databaseProvider;
        dbConnectionUrl = databaseProvider.createDatabase(DatabaseRule.class.getName());
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    statement.evaluate();
                } finally {
                    if (dbConnectionUrl != null) {
                        databaseProvider.shutdown();
                        databaseProvider = null;
                        dbConnectionUrl = null;
                    }
                }
            }
        };
    }

    public void createTable(String tableName, String... columns) throws SQLException {
        executeJdbc("CREATE TABLE " + tableName + " (" + String.join(", ", columns) + ")");
    }

    public void executeJdbc(String sql) throws SQLException {

        try (Connection conn = DriverManager.getConnection(dbConnectionUrl);
             java.sql.Statement stmt = conn.createStatement()
        ) {
            stmt.execute(sql);
        }
    }

    public void insertItems(String tableName, int count) throws SQLException {
        for (int i = 0; i < count; i++) {
            executeJdbc(String.format("INSERT INTO " + tableName + " VALUES(%d, 'name-%d')", i, i));
        }
    }
}
