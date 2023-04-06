/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datalink.impl;

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.datalink.DataLink;
import com.hazelcast.datalink.DataLinkBase;
import com.hazelcast.datalink.DataLinkResource;
import com.hazelcast.spi.annotation.Beta;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * {@link DataLink} implementation for JDBC.
 * <p>
 * Implementation is based on {@link HikariDataSource}. {@link DataLinkConfig#getProperties()} are passed directly
 * to {@link HikariConfig}. For available options see
 * <a href="https://github.com/brettwooldridge/HikariCP#gear-configuration-knobs-baby">HikariCP configuration</a>
 *
 * @since 5.3
 */
@Beta
public class JdbcDataLink extends DataLinkBase {

    private static final AtomicInteger DATA_SOURCE_COUNTER = new AtomicInteger();

    private HikariDataSource pooledDataSource;
    private Supplier<Connection> singleUseConnectionSup;

    public JdbcDataLink(DataLinkConfig config) {
        super(config);
        if (config.isShared()) {
            this.pooledDataSource = createHikariDataSource();
        } else {
            this.singleUseConnectionSup = createSingleConnectionSup();
        }
    }

    protected HikariDataSource createHikariDataSource() {
        Properties properties = new Properties();
        properties.putAll(getConfig().getProperties());
        if (!properties.containsKey("poolName")) {
            int cnt = DATA_SOURCE_COUNTER.getAndIncrement();
            properties.put("poolName", "HikariPool-" + cnt + "-" + getName());
        }
        HikariConfig dataSourceConfig = new HikariConfig(properties);
        return new HikariDataSource(dataSourceConfig);
    }

    private Supplier<Connection> createSingleConnectionSup() {
        Properties properties = getConfig().getProperties();
        String jdbcUrl = properties.getProperty("jdbcUrl");
        Properties connectionProps = new Properties();
        properties.entrySet().stream().filter(e -> !"jdbcUrl".equals(e.getKey()))
                  .forEach(e -> connectionProps.put(e.getKey(), e.getValue()));

        return () -> {
            try {
                return DriverManager.getConnection(jdbcUrl, connectionProps);
            } catch (SQLException e) {
                throw new HazelcastException("Could not create a new connection: " + e, e);
            }
        };
    }

    @Nonnull
    @Override
    public List<DataLinkResource> listResources() {
        try (Connection connection = getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            //Retrieving the columns in the database
            ResultSet tables = metaData.getTables(null, null, "%", null);
            List<DataLinkResource> result = new ArrayList<>();
            while (tables.next()) {
                result.add(new DataLinkResource(
                        "TABLE",
                        // TODO quoting? Using String[]?
                        tables.getString("TABLE_SCHEM") + "." + tables.getString("TABLE_NAME")
                ));
            }
            return result;
        } catch (Exception e) {
            throw new HazelcastException("Could not read resources for DataLink " + getName());
        }
    }

    private Connection pooledConnection() {
        retain();
        try {
            return new ConnectionDelegate(pooledDataSource.getConnection()) {
                @Override
                public void close() {
                    try {
                        super.close();
                    } catch (Exception e) {
                        throw new HazelcastException("Could not close connection", e);
                    } finally {
                        release();
                    }
                }
            };
        } catch (SQLException e) {
            throw new HazelcastException("Could not get Connection from pool", e);
        }
    }

    private Connection singleUseConnection() {
        // We don't call retain() for single-use connections. They are independent of the DataLink,
        // so closing the DataLink doesn't affect an active single-use connection.
        return singleUseConnectionSup.get();
    }

    /**
     * Return a {@link Connection} based on this DataLink configuration.
     * <p>
     * Depending on the {@link DataLinkConfig#isShared()} config the Connection is
     * - shared=true -> a new Connection is created each time the method is called
     * - shared=false -> a Connection is obtained from a pool, returned back to
     * the pool, when it is closed
     * <p>
     * The caller must close the Connection when finished to allow correct
     * release of the underlying resources. In case of a single-use connection, the
     * connection is closed immediately. For pooled connections the connection is
     * returned to the pool.
     */
    public Connection getConnection() {
        return getConfig().isShared() ? pooledConnection() : singleUseConnection();
    }

    @Override
    public void destroy() {
        if (pooledDataSource != null) {
            try {
                pooledDataSource.close();
            } catch (Exception e) {
                throw new HazelcastException("Could not close connection pool", e);
            }
            pooledDataSource = null;
        } else {
            singleUseConnectionSup = null;
        }
    }

    /**
     * For tests only
     */
    HikariDataSource pooledDataSource() {
        return pooledDataSource;
    }
}
