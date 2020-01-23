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

package com.hazelcast.sql.jdbc;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.jdbc.impl.JdbcGateway;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC driver.
 */
public class HazelcastJdbcDriver implements Driver {
    /** Prefix of JDBC driver. */
    private static final String URL_PREFIX = "jdbc:hazelcast://";

    /** Major version. */
    private static final int VER_MAJOR = 1;

    /** Minor version. */
    private static final int VER_MINOR = 0;

    /** Registered driver instance. */
    private static HazelcastJdbcDriver driver;

    /** Whether the driver is registered. */
    private static boolean registered;

    static {
        register();
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        // Null URL should throw an exception.
        if (url == null) {
            throw new SQLException("URL cannot be null.");
        }

        // Wrong driver.
        if (!acceptsURL(url)) {
            return null;
        }

        return connect0(url, info);
    }

    /**
     * Internal connection routine.
     *
     * @param url URL.
     * @param info Info.
     * @return Connection.
     * @throws SQLException If failed.
     */
    private HazelcastJdbcConnection connect0(String url, Properties info) throws SQLException {
        String hostPort = url.substring(URL_PREFIX.length());

        ClientNetworkConfig networkConfig = new ClientNetworkConfig().setAddresses(Collections.singletonList(hostPort));
        ClientConfig clientConfig = new ClientConfig().setNetworkConfig(networkConfig);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        JdbcGateway gateway = new JdbcGateway(client);

        return new HazelcastJdbcConnection(gateway);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.toLowerCase().startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        // TODO: Implement.
        throw new UnsupportedOperationException("getPropertyInfo");
    }

    @Override
    public int getMajorVersion() {
        return VER_MAJOR;
    }

    @Override
    public int getMinorVersion() {
        return VER_MINOR;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("The driver does not use java.util.logging");
    }

    /**
     * @return Driver instance.
     */
    public static synchronized Driver register() {
        if (registered) {
            assert driver != null;

            return driver;
        }

        try {
            HazelcastJdbcDriver driver0 = new HazelcastJdbcDriver();

            DriverManager.registerDriver(driver0);

            driver = driver0;
            registered = true;

            return driver;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register Ignite JDBC thin driver.", e);
        }
    }
}
