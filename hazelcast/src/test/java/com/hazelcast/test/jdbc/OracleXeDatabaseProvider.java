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
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.MountableFile;

import javax.sql.CommonDataSource;

import static com.hazelcast.test.HazelcastTestSupport.assumeNoArm64Architecture;

public class OracleXeDatabaseProvider extends JdbcDatabaseProvider<OracleContainer> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleXeDatabaseProvider.class);

    public final String dockerImageName;

    OracleXeDatabaseProvider(String dockerImageName) {
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
                .withCopyFileToContainer(MountableFile.forClasspathResource("oracle/oracle-xe-init.sql"), "/container-entrypoint-startdb.d/init.sql");
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
