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

package com.hazelcast.dataconnection.impl;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.dataconnection.DataConnection;
import com.hazelcast.dataconnection.DataConnectionBase;
import com.hazelcast.dataconnection.DataConnectionResource;
import com.hazelcast.dataconnection.impl.jdbcproperties.HikariTranslator;
import com.hazelcast.jet.impl.util.ConcurrentMemoizingSupplier;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.hazelcast.dataconnection.impl.DatabaseDialect.resolveDialect;
import static com.hazelcast.dataconnection.impl.jdbcproperties.DataConnectionProperties.JDBC_URL;
import static com.hazelcast.dataconnection.impl.jdbcproperties.DriverManagerTranslator.translate;
import static java.util.Locale.ROOT;

/**
 * {@link DataConnection} implementation for JDBC.
 * <p>
 * Implementation is based on {@link HikariDataSource}. {@link DataConnectionConfig#getProperties()} are passed directly
 * to {@link HikariConfig}. For available options see
 * <a href="https://github.com/brettwooldridge/HikariCP#gear-configuration-knobs-baby">HikariCP configuration</a>
 *
 * @since 5.3
 */
public class JdbcDataConnection extends DataConnectionBase {

    public static final String OBJECT_TYPE_TABLE = "Table";

    private static final List<String> H2_SYSTEM_SCHEMA_LIST = List.of("INFORMATION_SCHEMA");

    private static final List<String> MYSQL_SYSTEM_CATALOG_LIST = List.of("SYS");

    private static final List<String> MSSQL_SYSTEM_SCHEMA_LIST = List.of("sys", "INFORMATION_SCHEMA");
    private static final List<String> MSSQL_SYSTEM_TABLE_LIST = List.of("MSreplication_options", "spt_fallback_db",
            "spt_fallback_dev", "spt_fallback_usg", "spt_monitor", "spt_values");

    private static final AtomicInteger DATA_SOURCE_COUNTER = new AtomicInteger();
    private volatile ConcurrentMemoizingSupplier<HikariDataSource> pooledDataSourceSup;
    private volatile Supplier<Connection> singleUseConnectionSup;

    public JdbcDataConnection(DataConnectionConfig config) {
        super(config);
        if (config.isShared()) {
            this.pooledDataSourceSup = new ConcurrentMemoizingSupplier<>(this::createHikariDataSource);
        } else {
            this.singleUseConnectionSup = createSingleConnectionSup();
        }
    }

    protected HikariDataSource createHikariDataSource() {
        try {
            validate(getConfig());
            Properties properties = getConfig().getProperties();

            HikariTranslator translator = new HikariTranslator(DATA_SOURCE_COUNTER, getName());
            Properties translatedProperties = translator.translate(properties);

            HikariConfig dataSourceConfig = new HikariConfig(translatedProperties);

            return new HikariDataSource(dataSourceConfig);
        } catch (Exception e) {
            throw new HazelcastException("Could not create pool for data connection '" + getName() + "'", e);
        }
    }

    private Supplier<Connection> createSingleConnectionSup() {
        validate(getConfig());
        Properties properties = getConfig().getProperties();

        Properties translatedProperties = translate(properties);

        return () -> {
            try {
                String jdbcUrl = properties.getProperty(JDBC_URL);
                return DriverManager.getConnection(jdbcUrl, translatedProperties);
            } catch (SQLException e) {
                throw new HazelcastException("Could not create a new connection: " + e, e);
            }
        };
    }

    private void validate(DataConnectionConfig config) {
        Properties properties = config.getProperties();
        if (properties.get(JDBC_URL) == null) {
            throw new HazelcastException(JDBC_URL + " property is not defined for data connection '" + getName() + "'");
        }
    }

    @Nonnull
    @Override
    public Collection<String> resourceTypes() {
        return Collections.singleton(OBJECT_TYPE_TABLE);
    }

    @Nonnull
    @Override
    public List<DataConnectionResource> listResources() {
        try {
            try (Connection connection = getConnection()) {
                DatabaseMetaData databaseMetaData = connection.getMetaData();

                ResourceReader reader = new ResourceReader();
                switch (resolveDialect(databaseMetaData)) {
                    case H2:
                        reader.withCatalog(connection.getCatalog())
                                .exclude(
                                        (catalog, schema, table) ->
                                                H2_SYSTEM_SCHEMA_LIST.contains(schema)
                                );
                        break;

                    case POSTGRESQL:
                        reader.withCatalog(connection.getCatalog());
                        break;

                    case MYSQL:
                        reader.exclude(
                                (catalog, schema, table) ->
                                        catalog != null && MYSQL_SYSTEM_CATALOG_LIST.contains(catalog.toUpperCase(ROOT))
                        );
                        break;

                    case MICROSOFT_SQL_SERVER:
                        reader
                                .withCatalog(connection.getCatalog())
                                .exclude(
                                        (catalog, schema, table) ->
                                                MSSQL_SYSTEM_SCHEMA_LIST.contains(schema)
                                                || MSSQL_SYSTEM_TABLE_LIST.contains(table)
                                );
                        break;

                    default:
                        // Nothing to do
                }

                return reader.listResources(connection);
            }
        } catch (Exception exception) {
            throw new HazelcastException("Could not read resources for DataConnection " + getName(), exception);
        }
    }

    private Connection pooledConnection() {
        retain();
        try {
            return new ConnectionDelegate(pooledDataSourceSup.get().getConnection()) {
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
        // We don't call retain() for single-use connections. They are independent of the DataConnection,
        // so closing the DataConnection doesn't affect an active single-use connection.
        return singleUseConnectionSup.get();
    }

    /**
     * Return a {@link Connection} based on this DataConnection configuration.
     * <p>
     * Depending on the {@link DataConnectionConfig#isShared()} config the Connection is
     * - shared=true -> a new Connection is created each time the method is called
     * - shared=false -> a Connection is obtained from a pool, returned back to
     * the pool, when it is closed
     * <p>
     * When finished, the caller must close the Connection to ensure the proper release of the underlying resources.
     * If the connection is for single use, it is closed immediately.
     * For pooled connections, the connection is returned back to the pool.
     */
    public Connection getConnection() {
        return getConfig().isShared() ? pooledConnection() : singleUseConnection();
    }

    @Override
    public void destroy() {
        ConcurrentMemoizingSupplier<HikariDataSource> localPooledDataSourceSup = pooledDataSourceSup;
        if (localPooledDataSourceSup != null) {
            HikariDataSource dataSource = localPooledDataSourceSup.remembered();
            pooledDataSourceSup = null;
            if (dataSource != null) {
                try {
                    dataSource.close();
                } catch (Exception e) {
                    throw new HazelcastException("Could not close connection pool", e);
                }
            }
        }
        singleUseConnectionSup = null;
    }

    /**
     * For tests only
     */
    HikariDataSource pooledDataSource() {
        return pooledDataSourceSup.get();
    }
}
