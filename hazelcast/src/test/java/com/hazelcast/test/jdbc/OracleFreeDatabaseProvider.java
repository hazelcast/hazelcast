/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.MountableFile;

import javax.sql.CommonDataSource;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static com.hazelcast.test.HazelcastTestSupport.assumeNoArm64Architecture;

public class OracleFreeDatabaseProvider extends JdbcDatabaseProvider<OracleContainer> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleFreeDatabaseProvider.class);

    // The LogMessageWaitStrategy for the Oracle XE container was set to 240 seconds, but with the Oracle Free Container,
    // it has been reduced to 60 seconds, which is too short.
    // We need to use a longer wait strategy.
    private static final WaitStrategy WAIT_STRATEGY = new LogMessageWaitStrategy()
            .withRegEx(".*DATABASE IS READY TO USE!.*\\s")
            .withTimes(1)
            .withStartupTimeout(Duration.of(240, ChronoUnit.SECONDS));

    private final String dockerImageName;

    OracleFreeDatabaseProvider(String dockerImageName) {
        this.dockerImageName = dockerImageName;
    }

    @Override
    public CommonDataSource createDataSource(boolean xa) {
        throw new RuntimeException("Not supported");
    }

    @SuppressWarnings("resource")
    @Override
    OracleContainer createContainer(String dbName) {
        assumeNoArm64Architecture();
        return new OracleContainer(dockerImageName)
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .withCopyFileToContainer(MountableFile.forClasspathResource("oracle/oracle-free-init.sql"), "/container-entrypoint-startdb.d/init.sql")
                .waitingFor(WAIT_STRATEGY);
    }

    @Override
    public String url() {
        return "jdbc:oracle:thin:" + user() + "/" + password()
               + "@" + container.getHost() + ":" + container.getOraclePort() + "/" + container.getDatabaseName();
    }

    @Override
    public String noAuthJdbcUrl() {
        return container.getJdbcUrl()
                .replaceAll("&?user=" + user(), "")
                .replaceAll("&?password=" + password(), "");
    }

    @Override
    public String user() {
        return container.getUsername();
    }

    @Override
    public String password() {
        return container.getPassword();
    }

    @Override
    public TestDatabaseRecordProvider recordProvider() {
        return new OracleObjectProvider(this);
    }
}
