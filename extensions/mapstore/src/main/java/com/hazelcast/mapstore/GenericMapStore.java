/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.mapstore;

import com.hazelcast.dataconnection.impl.JdbcDataConnection;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.mapstore.JdbcParameters.convert;

/**
 * GenericMapStore is an implementation of {@link MapStore} built
 * on top of Hazelcast SQL engine.
 * <p>
 * It works with any SQL connector supporting SELECT, INSERT, UPDATE and DELETE statements.
 * <p>
 * Usage:
 * <p>
 * First define data connection, e.g. for JDBC use {@link JdbcDataConnection}:
 * <pre>{@code Config config = new Config();
 * config.addDataConnectionConfig(
 *   new DataConnectionConnection("mysql-ref")
 *     .setType("Jdbc")
 *     .setProperty("jdbcUrl", dbConnectionUrl)
 * );}</pre>
 * <p>
 * Then create a Map with {@link MapStore} using the GenericMapStore implementation:
 * <pre>{@code MapConfig mapConfig = new MapConfig(mapName);
 * MapStoreConfig mapStoreConfig = new MapStoreConfig();
 * mapStoreConfig.setClassName(GenericMapStore.class.getName());
 * mapStoreConfig.setProperty(GenericMapStore.DATA_CONNECTION_REF_PROPERTY, "mysql-name");
 * mapConfig.setMapStoreConfig(mapStoreConfig);
 * instance().getConfig().addMapConfig(mapConfig);}</pre>
 * <p>
 * The GenericMapStore creates a SQL mapping with name "__map-store." + mapName.
 * This mapping is removed when the map is destroyed.
 * <p>
 * Note : When GenericMapStore uses GenericRecord as value, even if the GenericRecord contains the primary key as a field,
 * the primary key is still received from @{link {@link com.hazelcast.map.IMap} method call
 *
 * @param <K> type of the key
 * @param <V> type of the value
 */
public class GenericMapStore<K, V> extends GenericMapLoader<K, V>
        implements MapStore<K, V>, MapLoaderLifecycleSupport {

    @Override
    public void store(K key, V value) {
        awaitSuccessfulInit();

        JdbcParameters jdbcParameters = convert(
                key,
                value,
                columnMetadataList,
                genericMapStoreProperties.idColumn,
                genericMapStoreProperties.singleColumnAsValue
        );

        try {
            sqlService.execute(queries.storeSink(), jdbcParameters.getParams()).close();
        } catch (Exception e) {

            if (isIntegrityConstraintViolation(e)) {

                // Try to update the row
                jdbcParameters.shiftIdParameterToEnd();

                String updateSQL = queries.storeUpdate();
                sqlService.execute(updateSQL, jdbcParameters.getParams()).close();
            } else {
                throw e;
            }
        }
    }

    @Override
    public void storeAll(Map<K, V> map) {
        awaitSuccessfulInit();

        for (Entry<K, V> entry : map.entrySet()) {
            store(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void delete(K key) {
        awaitSuccessfulInit();

        sqlService.execute(queries.delete(), key).close();
    }

    @Override
    public void deleteAll(Collection<K> keys) {
        awaitSuccessfulInit();

        if (keys.isEmpty()) {
            return;
        }

        sqlService.execute(queries.deleteAll(keys.size()), keys.toArray()).close();
    }

    // SQLException returns SQL state in five-digit number.
    // These five-digit numbers tell about the status of the SQL statements.
    // The SQLSTATE values consists of two fields.
    // The class, which is the first two characters of the string, and
    // the subclass, which is the terminating three characters of the string.
    // See https://en.wikipedia.org/wiki/SQLSTATE for cate
    static boolean isIntegrityConstraintViolation(Exception exception) {
        boolean result = false;
        SQLException sqlException = findSQLException(exception);
        if (sqlException != null) {
            String sqlState = sqlException.getSQLState();
            if (sqlState != null) {
                result = sqlState.startsWith("23");
            }
        }
        return result;
    }

    static SQLException findSQLException(Throwable throwable) {
        Throwable rootCause = throwable;
        while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
            rootCause = rootCause.getCause();
            if (rootCause instanceof SQLException) {
                return (SQLException) rootCause;
            }
        }
        return null;
    }
}
