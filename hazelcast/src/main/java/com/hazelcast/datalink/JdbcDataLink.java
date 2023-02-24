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
import com.hazelcast.datalink.impl.CloseableDataSource;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.jet.impl.connector.DataSourceFromConnectionSupplier;
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

/**
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

    private static final int JDBC_TEST_CONNECTION_TIMEOUT_SECONDS = 5;
    private static final AtomicInteger DATA_SOURCE_COUNTER = new AtomicInteger();

    /*
     * Reference counter to handle closing of the pooledDataSource:
     * - when a new JdbcDataLink is created it starts with counter=1
     * - everytime the DataLink is retrieved from the DataLinkService the counter increases
     * - the caller must call `close()` on this data link after it's done (e.g. when it retrieves the dataSource)
     * - everytime pooledDataSource is returned from this DataLink the counter increases
     * - when the pooledDataSource is closed, it's not actually closed, only the counter decreases
     * - the DataLink is closed when removed or on instance shutdown - leading to actual closing of
     * the pooledDataSource
     *
     * We increase the counter both in DataLinkService and DataLink to avoid race condition when
     * - a JdbcDataLink is retrieved from the service
     * - the DataLink is removed and the pooledDataSource is closed
     * - a closed pooledDataSource is returned
     */
    protected final ReferenceCounter refCounter;
    protected final DataLinkConfig config;

    protected CloseableDataSource pooledDataSource;
    protected DataSource singleUseDataSource;

    public JdbcDataLink(DataLinkConfig config) {
        this.refCounter = new ReferenceCounter(() -> {
            destroy();
            return null;
        });
        this.config = config;
        this.pooledDataSource = createHikariDataSource();
        this.singleUseDataSource = createSingleUseDataSource();
    }

    private DataSource createSingleUseDataSource() {
        Properties properties = config.getProperties();
        String jdbcUrl = properties.getProperty("jdbcUrl");
        return new DataSourceFromConnectionSupplier(
                () -> {
                    try {
                        Properties connectionProps = new Properties();
                        properties.entrySet().stream().filter(e -> !"jdbcUrl".equals(e.getKey()))
                                  .forEach(e -> connectionProps.put(e.getKey(), e.getValue()));
                        return DriverManager.getConnection(jdbcUrl, connectionProps);
                    } catch (SQLException e) {
                        throw new HazelcastException("Could not create a new connection", e);
                    }
                }
        );
    }

    @Override
    public String getName() {
        return config.getName();
    }

    @Override
    public List<Resource> listResources() {
        try (Connection connection = singleUseDataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            String[] types = {"TABLE", "VIEW"};
            //Retrieving the columns in the database
            ResultSet tables = metaData.getTables(null, null, "%", types);
            List<Resource> result = new ArrayList<>();
            while (tables.next()) {
                result.add(new Resource(
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

    public DataSource pooledDataSource() {
        retain();
        return pooledDataSource;
    }

    public DataSource singleUseDataSource() {
        return singleUseDataSource;
    }

    public DataSource getDataSource() {
        return config.isShared() ? pooledDataSource() : singleUseDataSource();
    }

    protected CloseableDataSource createHikariDataSource() {
        Properties properties = new Properties();
        properties.putAll(config.getProperties());
        if (!properties.containsKey("poolName")) {
            String suffix = StringUtil.isNullOrEmpty(config.getName()) ? "" : "-" + config.getName();
            properties.put("poolName", "HikariPool-" + DATA_SOURCE_COUNTER.getAndIncrement() + suffix);
        }
        HikariConfig dataSourceConfig = new HikariConfig(properties);
        return new CloseableDataSource(new HikariDataSource(dataSourceConfig)) {

            @Override
            public void close() throws Exception {
                refCounter.release();
            }
        };
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
                pooledDataSource.getDataSource().unwrap(HikariDataSource.class).close();
            } catch (Exception e) {
                throw new HazelcastException("Could not close connection pool", e);
            }
            pooledDataSource = null;
        }
    }
}
