package com.hazelcast.test.jdbc;

public interface TestDatabaseProvider {

    String createDatabase(String dbName);

    void shutdown();
}
