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

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.datastore.ExternalDataStoreFactory;
import com.hazelcast.datastore.JdbcDataStoreFactory;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.internal.util.executor.ManagedExecutorService;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlConnector.OPTION_EXTERNAL_DATASTORE_REF;
import static com.hazelcast.mapstore.GenericMapStoreProperties.MAPPING_TYPE_PROPERTY;
import static com.hazelcast.sql.SqlRowMetadata.COLUMN_NOT_FOUND;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Stream.of;

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
        implements MapStore<K, GenericRecord>, MapLoaderLifecycleSupport {

    /**
     * Timeout for initialization of GenericMapStore
     */
    public static final HazelcastProperty MAPSTORE_INIT_TIMEOUT
            = new HazelcastProperty("hazelcast.mapstore.init.timeout", 5, SECONDS);

    static final String MAPPING_PREFIX = "__map-store.";

    static final String MAPPING_NAME_COLUMN = "name";

    static final String H2_PK_VIOLATION = "Unique index or primary key violation";
    static final String PG_PK_VIOLATION = "ERROR: duplicate key value violates unique constraint";
    static final String MYSQL_PK_VIOLATION = "Duplicate entry";

    private ILogger logger;

    private HazelcastInstanceImpl instance;

    private String mapName;
    private String mapping;

    private long initTimeoutMillis;

    private Exception initFailure; // uses initFinished latch to ensure visibility
    private List<SqlColumnMetadata> columnMetadataList;
    private final CountDownLatch initFinished = new CountDownLatch(1);

    @Override
    public void init(HazelcastInstance instance, Properties properties, String mapName) {
        verifyMapStoreOffload(instance, mapName);

        logger = instance.getLoggingService().getLogger(GenericMapStore.class);

        this.instance = Util.getHazelcastInstanceImpl(instance);

        initMapLoader(this.instance, properties, mapName);

        this.mapName = mapName;
        this.mapping = MAPPING_PREFIX + mapName;

        HazelcastProperties hzProperties = nodeEngine().getProperties();
        this.initTimeoutMillis = hzProperties.getMillis(MAPSTORE_INIT_TIMEOUT);

        ManagedExecutorService asyncExecutor = nodeEngine()
                .getExecutionService()
                .getExecutor(ExecutionService.MAP_STORE_OFFLOADABLE_EXECUTOR);

        // Init can run on partition thread, creating a mapping uses other maps, so it needs to run elsewhere
        asyncExecutor.submit(() -> {
            // We create the mapping only on the master node
            // On other members we wait until the mapping has been created
            createMappingForMapStore(mapName);
        });
    }

    private void verifyMapStoreOffload(HazelcastInstance instance, String mapName) {
        MapConfig mapConfig = instance.getConfig().findMapConfig(mapName);
        if (!mapConfig.getMapStoreConfig().isOffload()) {
            throw new HazelcastException("Config for GenericMapStore must have `offload` property set to true");
        }
    }

    private NodeEngineImpl nodeEngine() {
        return instance.node.nodeEngine;
    }

    private void createMappingForMapStore(String mapName) {
        logger.fine("Initializing for map " + mapName);
        try {
            String mappingColumns = null;
            if (properties.hasColumns()) {
                mappingColumns = resolveMappingColumns(properties.tableName, properties.externalDataStoreRef);
                logger.fine("Discovered following mapping columns: " + mappingColumns);
            }

            sql.execute(
                    "CREATE MAPPING \"" + mapping + "\" "
                            + "EXTERNAL NAME \"" + properties.tableName + "\" "
                            + (mappingColumns != null ? " ( " + mappingColumns + " ) " : "")
                            + "TYPE " + deriveMappingType() + " "
                            + "OPTIONS ("
                            + "    '" + OPTION_EXTERNAL_DATASTORE_REF + "' = '" + properties.externalDataStoreRef + "' "
                            + ")"
            ).close();

            if (!properties.hasColumns()) {
                columnMetadataList = loadMetadataFromMapping(mapping).getColumns();
            }
            queries = new Queries(mapping, properties.idColumn, columnMetadataList);
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().startsWith("Mapping or view already exists:")) {
                readExistingMapping();
            } else {
                logger.warning(e);
                initFailure = e;
            }
        } finally {
            initFinished.countDown();
        }
    }

    private String deriveMappingType() {
        if (properties.mappingType != null) {
            return properties.mappingType;
        } else {
            ExternalDataStoreFactory<?> factory = nodeEngine()
                    .getExternalDataStoreService()
                    .getExternalDataStoreFactory(properties.externalDataStoreRef);

            if (factory instanceof JdbcDataStoreFactory) {
                return "JDBC";
            } else {
                throw new HazelcastException("Unknown ExternalDataStoreFactory class " + factory.getClass()
                        + ". Set the mapping type using '" + MAPPING_TYPE_PROPERTY + "' property");
            }
        }
    }

    private String resolveMappingColumns(String tableName, String externalDataStoreRef) {
        String tempMapping = "temp_mapping_" + UuidUtil.newUnsecureUuidString();
        createMapping(tempMapping, tableName, externalDataStoreRef);
        SqlRowMetadata rowMetadata = loadMetadataFromMapping(tempMapping);
        columnMetadataList = rowMetadata.getColumns();
        dropMapping(tempMapping);

        return Stream.concat(of(properties.idColumn), properties.columns.stream())
                     .distinct() // avoid duplicate id column if present in columns property
                     .map(columnName -> validateColumn(rowMetadata.findColumn(columnName), columnName))
                     .map(rowMetadata::getColumn)
                     .map(columnMetadata1 -> columnMetadata1.getName() + " " + columnMetadata1.getType())
                     .collect(Collectors.joining(", "));
    }

    private void createMapping(String mappingName, String tableName, String externalDataStoreRef) {
        sql.execute(
                "CREATE MAPPING \"" + mappingName + "\""
                        + " EXTERNAL NAME \"" + tableName + "\" "
                        + " TYPE " + deriveMappingType()
                        + " OPTIONS ("
                        + "    '" + OPTION_EXTERNAL_DATASTORE_REF + "' = '" + externalDataStoreRef + "' "
                        + ")"
        ).close();
    }

    private SqlRowMetadata loadMetadataFromMapping(String mapping) {
        try (SqlResult result = sql.execute("SELECT * FROM \"" + mapping + "\" LIMIT 0")) {
            SqlRowMetadata rowMetadata = result.getRowMetadata();
            return rowMetadata;
        }
    }

    private void readExistingMapping() {
        logger.fine("Reading existing mapping for map" + mapName);
        try (SqlResult mappings = sql.execute("SHOW MAPPINGS")) {
            for (SqlRow mapping : mappings) {
                String name = mapping.getObject(MAPPING_NAME_COLUMN);
                if (name.equals(this.mapping)) {
                    SqlRowMetadata rowMetadata = loadMetadataFromMapping(name);
                    validateColumns(rowMetadata);
                    columnMetadataList = rowMetadata.getColumns();
                    queries = new Queries(name, properties.idColumn, columnMetadataList);
                    break;
                }
            }
        } catch (Exception e) {
            initFailure = e;
        }
    }

    private void validateColumns(SqlRowMetadata rowMetadata) {
        Stream.concat(of(properties.idColumn), properties.columns.stream())
              .distinct() // avoid duplicate id column if present in columns property
              .forEach((columnName) -> validateColumn(rowMetadata.findColumn(columnName), columnName));
    }

    private int validateColumn(int column, String columnName) {
        if (column == COLUMN_NOT_FOUND) {
            throw new HazelcastException("Column '" + columnName + "' not found");
        }
        return column;
    }

    @Override
    public void destroy() {
        awaitInitFinished();
        dropMapping(mapping);
    }

    @Override
    public GenericRecord load(K key) {
        awaitInitFinished();

        return super.load(key);
    }

    /**
     * Size of the {@code keys} collection is limited by {@link ClusterProperty#MAP_LOAD_CHUNK_SIZE}
     */
    @Override
    public Map<K, GenericRecord> loadAll(Collection<K> keys) {
        awaitInitFinished();

        return super.loadAll(keys);

    }

    @Override
    public Iterable<K> loadAllKeys() {
        awaitInitFinished();

        return super.loadAllKeys();

    }

    @Override
    public void store(K key, GenericRecord genericRecord) {
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
                    params[i] = genericRecord.getString(columnMetadata.getName());
                    break;

                case BOOLEAN:
                    params[i] = genericRecord.getBoolean(columnMetadata.getName());
                    break;

                case TINYINT:
                    params[i] = genericRecord.getInt8(columnMetadata.getName());
                    break;

                case SMALLINT:
                    params[i] = genericRecord.getInt16(columnMetadata.getName());
                    break;

                case INTEGER:
                    params[i] = genericRecord.getInt32(columnMetadata.getName());
                    break;

                case BIGINT:
                    params[i] = genericRecord.getInt64(columnMetadata.getName());
                    break;

                case REAL:
                    params[i] = genericRecord.getFloat32(columnMetadata.getName());
                    break;

                case DOUBLE:
                    params[i] = genericRecord.getFloat64(columnMetadata.getName());
                    break;

                case DATE:
                    params[i] = genericRecord.getDate(columnMetadata.getName());
                    break;

                case TIME:
                    params[i] = genericRecord.getTime(columnMetadata.getName());
                    break;

                case TIMESTAMP:
                    params[i] = genericRecord.getTimestamp(columnMetadata.getName());
                    break;

                case TIMESTAMP_WITH_TIME_ZONE:
                    params[i] = genericRecord.getTimestampWithTimezone(columnMetadata.getName());
                    break;

                case DECIMAL:
                    params[i] = genericRecord.getDecimal(columnMetadata.getName());
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

    private void dropMapping(String mappingName) {
        sql.execute("DROP MAPPING IF EXISTS \"" + mappingName + "\"").close();
    }

    /**
     * Awaits successful initialization, if the initialization failed it throws an exception
     */
    private void awaitInitFinished() {
        try {
            boolean finished = initFinished.await(initTimeoutMillis, MILLISECONDS);
            if (!finished) {
                throw new HazelcastException("MapStore init for map: " + mapName + " timed out after " + initTimeoutMillis
                        + " ms", initFailure);
            }
            if (initFailure != null) {
                throw new HazelcastException("MapStore init failed for map: " + mapName, initFailure);
            }
        } catch (InterruptedException e) {
            throw new HazelcastException(e);
        }
    }

}
