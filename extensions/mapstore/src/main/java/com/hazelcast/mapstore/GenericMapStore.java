/*
 * Copyright 2021 Hazelcast Inc.
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


import com.hazelcast.core.HazelcastException;
import com.hazelcast.map.MapStore;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlResult;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

/**
 * GenericMapStore is an implementation of {@link MapStore} built
 * on top of Hazelcast SQL engine.
 * <p>
 * It works with any SQL connector supporting SELECT, INSERT, UPDATE and DELETE statements.
 * <p>
 * Usage:
 * <p>
 * First define external data store, e.g. for JDBC use {@link com.hazelcast.datastore.JdbcDataStoreFactory}:
 * <pre>{@code Config config = new Config();
 * config.addExternalDataStoreConfig(
 *   new ExternalDataStoreConfig("mysql-ref")
 *     .setClassName(JdbcDataStoreFactory.class.getName())
 *     .setProperty("jdbcUrl", dbConnectionUrl)
 * );}</pre>
 * <p>
 * Then create a Map with {@link MapStore} using the GenericMapStore implementation:
 * <pre>{@code MapConfig mapConfig = new MapConfig(mapName);
 * MapStoreConfig mapStoreConfig = new MapStoreConfig();
 * mapStoreConfig.setClassName(GenericMapStore.class.getName());
 * mapStoreConfig.setProperty(OPTION_EXTERNAL_DATASTORE_REF, "mysql-ref");
 * mapConfig.setMapStoreConfig(mapStoreConfig);
 * instance().getConfig().addMapConfig(mapConfig);}</pre>
 * <p>
 * The GenericMapStore creates a SQL mapping with name "__map-store." + mapName.
 * This mapping is removed when the map is destroyed.
 *
 * @param <K>
 */
public class GenericMapStore<K> extends GenericMapLoader<K>
        implements MapStore<K, GenericRecord> {

    static final String H2_PK_VIOLATION = "Unique index or primary key violation";
    static final String PG_PK_VIOLATION = "ERROR: duplicate key value violates unique constraint";
    static final String MYSQL_PK_VIOLATION = "Duplicate entry";


    @Override
    public void store(K key, GenericRecord record) {
        awaitInitFinished();

        int idPos = -1;
        Object[] params = new Object[columnMetadataList.size()];
        for (int i = 0; i < columnMetadataList.size(); i++) {
            SqlColumnMetadata columnMetadata = columnMetadataList.get(i);
            if (columnMetadata.getName().equals(properties.idColumn)) {
                idPos = i;
            }
            switch (columnMetadata.getType()) {
                case VARCHAR:
                    params[i] = record.getString(columnMetadata.getName());
                    break;

                case BOOLEAN:
                    params[i] = record.getBoolean(columnMetadata.getName());
                    break;

                case TINYINT:
                    params[i] = record.getInt8(columnMetadata.getName());
                    break;

                case SMALLINT:
                    params[i] = record.getInt16(columnMetadata.getName());
                    break;

                case INTEGER:
                    params[i] = record.getInt32(columnMetadata.getName());
                    break;

                case BIGINT:
                    params[i] = record.getInt64(columnMetadata.getName());
                    break;

                case REAL:
                    params[i] = record.getFloat32(columnMetadata.getName());
                    break;

                case DOUBLE:
                    params[i] = record.getFloat64(columnMetadata.getName());
                    break;

                case DATE:
                    params[i] = record.getDate(columnMetadata.getName());
                    break;

                case TIME:
                    params[i] = record.getTime(columnMetadata.getName());
                    break;

                case TIMESTAMP:
                    params[i] = record.getTimestamp(columnMetadata.getName());
                    break;

                case TIMESTAMP_WITH_TIME_ZONE:
                    params[i] = record.getTimestampWithTimezone(columnMetadata.getName());
                    break;

                case DECIMAL:
                    params[i] = record.getDecimal(columnMetadata.getName());
                    break;

                default:
                    throw new HazelcastException("Column type " + columnMetadata.getType() + " not supported");
            }
        }
        try (SqlResult ignored = sql.execute(queries.storeInsert(), params)) {
        } catch (Exception e) {
            if (e.getMessage() != null && (e.getMessage().contains(H2_PK_VIOLATION) ||
                    e.getMessage().contains(PG_PK_VIOLATION) ||
                    e.getMessage().contains(MYSQL_PK_VIOLATION))) {
                Object tmp = params[idPos];
                params[idPos] = params[params.length - 1];
                params[params.length - 1] = tmp;
                sql.execute(queries.storeUpdate(), params).close();
            } else {
                throw e;
            }
        }
    }

    @Override
    public void storeAll(Map<K, GenericRecord> map) {
        awaitInitFinished();

        for (Entry<K, GenericRecord> entry : map.entrySet()) {
            store(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void delete(K key) {
        awaitInitFinished();

        sql.execute(queries.delete(), key).close();
    }

    @Override
    public void deleteAll(Collection<K> keys) {
        awaitInitFinished();

        if (keys.isEmpty()) {
            return;
        }

        sql.execute(queries.deleteAll(keys.size()), keys.toArray()).close();
    }

}
