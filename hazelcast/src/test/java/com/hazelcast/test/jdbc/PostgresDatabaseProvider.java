package com.hazelcast.test.jdbc;

import org.testcontainers.jdbc.ContainerDatabaseDriver;

public class PostgresDatabaseProvider implements TestDatabaseProvider {

    private String jdbcUrl;

    @Override
    public String createDatabase(String dbName) {
        jdbcUrl = "jdbc:tc:postgresql:10.21:///" + dbName + "?TC_DAEMON=true";
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
