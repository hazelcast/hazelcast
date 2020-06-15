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

package com.hazelcast.jet.cdc.mysql;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hazelcast.jet.cdc.AbstractIntegrationTest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import org.junit.Rule;
import org.testcontainers.containers.MySQLContainer;

import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;

public class AbstractMySqlIntegrationTest extends AbstractIntegrationTest {

    protected static final String DATABASE = "testDb";
    protected static final String SINK_MAP_NAME = "resultsMap";

    @Rule
    public MySQLContainer<?> mysql = new MySQLContainer<>("debezium/example-mysql:1.2")
            .withUsername("mysqluser")
            .withPassword("mysqlpw");

    protected MySqlCdcSources.Builder sourceBuilder() {
        return sourceBuilder("cdcMysql");
    }

    protected MySqlCdcSources.Builder sourceBuilder(String name) {
        return MySqlCdcSources.mysql(name)
                .setDatabaseAddress(mysql.getContainerIpAddress())
                .setDatabasePort(mysql.getMappedPort(MYSQL_PORT))
                .setDatabaseUser("debezium")
                .setDatabasePassword("dbz")
                .setClusterName("dbserver1");
    }

    protected void createDb(String name) throws SQLException {
        String jdbcUrl = "jdbc:mysql://" + mysql.getContainerIpAddress() + ":" + mysql.getMappedPort(MYSQL_PORT) + "/";
        try (Connection connection = DriverManager.getConnection(jdbcUrl, "root", "mysqlpw")) {
            connection
                    .prepareStatement("CREATE DATABASE " + name)
                    .executeUpdate();
            connection
                    .prepareStatement("GRANT ALL PRIVILEGES ON " + name + ".* TO 'mysqluser'@'%'")
                    .executeUpdate();
        }
    }

    protected static class TableRow {

        @JsonProperty("id")
        public int id;

        @JsonProperty("value_1")
        public String value1;

        @JsonProperty("value_2")
        public String value2;

        @JsonProperty("value_3")
        public String value3;

        TableRow() {
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, value1, value2, value3);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            TableRow other = (TableRow) obj;
            return id == other.id
                    && Objects.equals(value1, other.value1)
                    && Objects.equals(value2, other.value2)
                    && Objects.equals(value3, other.value3);
        }

        @Override
        public String toString() {
            return "TableRow {id=" + id + ", value1=" + value1 + ", value2=" + value2 + ", value3=" + value3 + '}';
        }
    }
}
