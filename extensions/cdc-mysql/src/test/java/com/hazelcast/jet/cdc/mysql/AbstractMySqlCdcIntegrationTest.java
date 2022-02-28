/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.cdc.AbstractCdcIntegrationTest;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.test.IgnoreInJenkinsOnWindows;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;

@Category({ParallelJVMTest.class, IgnoreInJenkinsOnWindows.class})
@RunWith(HazelcastSerialClassRunner.class)
public abstract class AbstractMySqlCdcIntegrationTest extends AbstractCdcIntegrationTest {

    public static final DockerImageName DOCKER_IMAGE = DockerImageName.parse("debezium/example-mysql:1.3")
            .asCompatibleSubstituteFor("mysql");

    @Rule
    public MySQLContainer<?> mysql = namedTestContainer(
            new MySQLContainer<>(DOCKER_IMAGE)
                    .withUsername("mysqluser")
                    .withPassword("mysqlpw")
    );

    @Before
    public void ignoreOnJdk15OrHigher() {
        Assume.assumeFalse("https://github.com/hazelcast/hazelcast-jet/issues/2623, " +
                        "https://github.com/hazelcast/hazelcast/issues/18800",
                System.getProperty("java.version").matches("^1[56].*"));
    }

    protected MySqlCdcSources.Builder sourceBuilder(String name) {
        return MySqlCdcSources.mysql(name)
                .setDatabaseAddress(mysql.getContainerIpAddress())
                .setDatabasePort(mysql.getMappedPort(MYSQL_PORT))
                .setDatabaseUser("debezium")
                .setDatabasePassword("dbz")
                .setClusterName("dbserver1")
                .setReconnectBehavior(RetryStrategies.indefinitely(1000));
    }

    protected void createDb(String database) throws SQLException {
        String jdbcUrl = "jdbc:mysql://" + mysql.getContainerIpAddress() + ":" + mysql.getMappedPort(MYSQL_PORT) + "/";
        try (Connection connection = getMySqlConnection(jdbcUrl, "root", "mysqlpw")) {
            Statement statement = connection.createStatement();
            statement.addBatch("CREATE DATABASE " + database);
            statement.addBatch("GRANT ALL PRIVILEGES ON " + database + ".* TO 'mysqluser'@'%'");
            statement.executeBatch();
        }
    }

    static Connection getConnection(MySQLContainer<?> mysql, String database) throws SQLException {
        return getMySqlConnection(
                mysql.withDatabaseName(database).getJdbcUrl(),
                mysql.getUsername(),
                mysql.getPassword()
        );
    }

}
