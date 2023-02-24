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
import java.sql.DriverManager;
import java.sql.SQLException;
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
 * @since 5.2
 */
@Beta
public class JdbcDataLink implements DataLink {

    private static final int JDBC_TEST_CONNECTION_TIMEOUT_SECONDS = 5;
    private static final AtomicInteger DATA_SOURCE_COUNTER = new AtomicInteger();

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

    @Override
    public String getName() {
        return config.getName();
    }

    private DataSource createSingleUseDataSource() {
        Properties properties = config.getProperties();
        String jdbcUrl = properties.getProperty("jdbcUrl");
        return new DataSourceFromConnectionSupplier(
                () -> {
                    try {
                        // TODO pass other properties
                        return DriverManager.getConnection(jdbcUrl);
                    } catch (SQLException e) {
                        throw new HazelcastException("Could not create a new connection", e);
                    }
                }
        );
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
