package com.hazelcast.test.jdbc;

import org.testcontainers.jdbc.ContainerDatabaseDriver;

public class MySQLDatabaseProvider implements TestDatabaseProvider {

    private String jdbcUrl;

    @Override
    public String createDatabase(String dbName) {
        jdbcUrl = "jdbc:tc:mysql:8.0.29:///" + dbName + "?TC_DAEMON=true&sessionVariables=sql_mode=ANSI";
        return jdbcUrl;
    }

    @Override
    public void shutdown() {
        if (jdbcUrl != null) {
            ContainerDatabaseDriver.killContainer(jdbcUrl);
            jdbcUrl = null;
        }
    }
}
