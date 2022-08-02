package com.hazelcast.test.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class H2DatabaseProvider implements TestDatabaseProvider {

    private String jdbcUrl;

    @Override
    public String createDatabase(String dbName) {
        jdbcUrl = "jdbc:h2:mem:" + dbName + ";DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";
        return jdbcUrl;
    }

    @Override
    public void shutdown() {
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            conn.createStatement().execute("shutdown");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
