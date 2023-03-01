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

package com.hazelcast.datalink;

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.datalink.impl.ConnectionDelegate;
import com.hazelcast.datalink.impl.ReferenceCounter;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.spi.annotation.Beta;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
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
 * TODO move this class to impl package when changing {@link DataLinkConfig#className} to type
 * <p>
 * Creates a JDBC data link as a {@link DataSource}.
 * <p>
 * Implementation is based on {@link HikariDataSource}. {@link DataLinkConfig#getProperties()} are passed directly
 * to {@link HikariConfig}. For available options see
 * <a href="https://github.com/brettwooldridge/HikariCP#gear-configuration-knobs-baby">HikariCP configuration</a>
 * </p>
 *
 * @since 5.3
 */
@Beta
public class JdbcDataLink implements DataLink {

    private static final AtomicInteger DATA_SOURCE_COUNTER = new AtomicInteger();

    /**
     * See {@link ReferenceCounter} for details
     */
    protected final ReferenceCounter refCounter;
    protected final DataLinkConfig config;

    protected HikariDataSource pooledDataSource;
    protected Supplier<Connection> singleUseConnectionSup;

    public JdbcDataLink(DataLinkConfig config) {
        this.refCounter = new ReferenceCounter(this::destroy);
        this.config = config;
        this.pooledDataSource = createHikariDataSource();
        this.singleUseConnectionSup = createSingleConnectionSup();
    }

    protected HikariDataSource createHikariDataSource() {
        Properties properties = new Properties();
        properties.putAll(config.getProperties());
        if (!properties.containsKey("poolName")) {
            String suffix = StringUtil.isNullOrEmpty(config.getName()) ? "" : "-" + config.getName();
            properties.put("poolName", "HikariPool-" + DATA_SOURCE_COUNTER.getAndIncrement() + suffix);
        }
        HikariConfig dataSourceConfig = new HikariConfig(properties);
        return new HikariDataSource(dataSourceConfig);
    }

    private Supplier<Connection> createSingleConnectionSup() {
        Properties properties = config.getProperties();
        String jdbcUrl = properties.getProperty("jdbcUrl");
        Properties connectionProps = new Properties();
        properties.entrySet().stream().filter(e -> !"jdbcUrl".equals(e.getKey()))
                  .forEach(e -> connectionProps.put(e.getKey(), e.getValue()));

        return () -> {
            try {
                return DriverManager.getConnection(jdbcUrl, connectionProps);
            } catch (SQLException e) {
                throw new HazelcastException("Could not create a new connection", e);
            }
        };
    }

    @Override
    public String getName() {
        return config.getName();
    }

    @Override
    public List<DataLinkResource> listResources() {
        try (Connection connection = singleUseConnectionSup.get()) {
            DatabaseMetaData metaData = connection.getMetaData();
            String[] types = {"TABLE", "VIEW"};
            //Retrieving the columns in the database
            ResultSet tables = metaData.getTables(null, null, "%", types);
            List<DataLinkResource> result = new ArrayList<>();
            while (tables.next()) {
                result.add(new DataLinkResource(
                        tables.getString("TABLE_TYPE"),
                        tables.getString("TABLE_SCHEM") + "." + tables.getString("TABLE_NAME")
                ));
            }
            return result;
        } catch (Exception e) {
            throw new HazelcastException("Could not read resources for DataLink " + config.getName());
        }
    }

    @Override
    public DataLinkConfig getConfig() {
        return config;
    }

    private Connection pooledConnection() {
        retain();
        try {
            return new ConnectionDelegate(pooledDataSource.getConnection()) {
                @Override
                public void close() throws SQLException {
                    super.close();
                    try {
                        refCounter.release();
                    } catch (Exception e) {
                        throw new HazelcastException("Could not close connection", e);
                    }
                }
            };
        } catch (SQLException e) {
            throw new HazelcastException("Could not get Connection from pool", e);
        }
    }

    private Connection singleUseConnection() {
        return singleUseConnectionSup.get();
    }

    /**
     * Return a {@link Connection} based this DataLink configuration.
     * <p>
     * Depending on the {@link DataLinkConfig#isShared()} config the Connection is
     * - shared=true -> a new Connection is created each time the method is called
     * - shared=false -> a Connection is obtained from a pool, returned back to
     * the pool, when it is closed
     * <p>
     * The caller must close the Connection when finished to allow correct
     * release of the underlying resources. In case of a single use connection, the
     * connection is closed immediately. For pooled connections the connection is
     * returned to the pool.
     */
    public Connection getConnection() {
        return config.isShared() ? pooledConnection() : singleUseConnection();
    }

    @Override
    public void retain() {
        refCounter.retain();
    }

    @Override
    public void close() throws Exception {
        refCounter.release();
    }

    protected void destroy() {
        if (pooledDataSource != null) {
            try {
                pooledDataSource.unwrap(HikariDataSource.class).close();
            } catch (Exception e) {
                throw new HazelcastException("Could not close connection pool", e);
            }
            pooledDataSource = null;
        }
    }

    /**
     * For tests only
     */
    HikariDataSource pooledDataSource() {
        return pooledDataSource;
    }
}
