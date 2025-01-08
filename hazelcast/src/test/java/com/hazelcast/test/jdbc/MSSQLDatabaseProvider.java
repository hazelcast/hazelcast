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

package com.hazelcast.test.jdbc;

import com.hazelcast.internal.tpcengine.util.OS;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerXADataSource;
import org.testcontainers.containers.MSSQLServerContainer;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

import static com.hazelcast.jet.TestedVersions.TEST_MSSQLSERVER_VERSION;

public class MSSQLDatabaseProvider extends JdbcDatabaseProvider<MSSQLServerContainer<?>> {

    @Override
    MSSQLServerContainer<?> createContainer(String dbName) {
        MSSQLServerContainer<?> mssqlServerContainer;
        if (isArmArchitecture()) {
            mssqlServerContainer = createAzureSQLEdgeContainer();
        } else {
            mssqlServerContainer = createMSSQLContainer();
        }
        mssqlServerContainer.acceptLicense()
                // See https://learn.microsoft.com/en-us/sql/connect/jdbc/using-basic-data-types?view=sql-server-ver16
                // "To use java.sql.Time with the time SQL Server type, you must set the sendTimeAsDatetime
                // connection property to false."
                .withUrlParam("sendTimeAsDateTime", "false")
                .withUrlParam("user", mssqlServerContainer.getUsername())
                .withUrlParam("password", mssqlServerContainer.getPassword());
        return mssqlServerContainer;
    }

    private boolean isArmArchitecture() {
        return "aarch64".equals(OS.osArch());
    }

    private MSSQLServerContainer<?> createAzureSQLEdgeContainer() {
        return new AzureSQLEdgeContainerProvider().newInstance();
    }

    private MSSQLServerContainer<?> createMSSQLContainer() {
        // withDatabaseName() throws UnsupportedOperationException
        return new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:" + TEST_MSSQLSERVER_VERSION);
    }

    @Override
    public DataSource createDataSource(boolean xa) {
        if (xa) {
            return createXADataSource();
        } else {
            return createDataSource();
        }
    }

    @Nonnull
    private SQLServerDataSource createDataSource() {
        SQLServerDataSource dataSource = new SQLServerDataSource();
        dataSource.setURL(url());
        dataSource.setUser(user());
        dataSource.setPassword(password());
        dataSource.setDatabaseName(getDatabaseName());
        return dataSource;
    }

    @Nonnull
    private SQLServerXADataSource createXADataSource() {
        SQLServerXADataSource dataSource = new SQLServerXADataSource();
        dataSource.setURL(url());
        dataSource.setUser(user());
        dataSource.setPassword(password());
        dataSource.setDatabaseName(getDatabaseName());
        return dataSource;
    }


    @Override
    public String getDatabaseName() {
        return "master";
    }


    @Override
    public String noAuthJdbcUrl() {
        return container.getJdbcUrl()
                .replaceAll(";user=" + user(), "")
                .replaceAll(";password=" + password(), "");
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
        return new MSSQLObjectProvider(this);
    }

}
