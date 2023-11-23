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

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.dataconnection.impl.JdbcDataConnection;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.internal.util.executor.ManagedExecutorService;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.StringUtil.isBoolean;
import static com.hazelcast.mapstore.ExistingMappingValidator.validateColumn;
import static com.hazelcast.mapstore.ExistingMappingValidator.validateColumnsExist;
import static com.hazelcast.mapstore.FromSqlRowConverter.toGenericRecord;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

/**
 * GenericMapLoader is an implementation of {@link MapLoader} built
 * on top of Hazelcast SQL engine.
 * <p>
 * It works with any SQL connector supporting SELECT statements.
 * <p>
 * Usage:
 * <p>
 * First define data connection, e.g. for JDBC use {@link JdbcDataConnection}:
 * <pre>{@code Config config = new Config();
 * config.addDataConnectionConfig(
 *   new DataConnectionConfig("mysql-ref")
 *     .setType("Jdbc")
 *     .setProperty("jdbcUrl", dbConnectionUrl)
 * );}</pre>
 * <p>
 * Then create a Map with {@link MapLoader} using the GenericMapLoader implementation:
 * <pre>{@code MapConfig mapConfig = new MapConfig(mapName);
 * MapStoreConfig mapStoreConfig = new MapStoreConfig();
 * mapStoreConfig.setClassName(GenericMapLoader.class.getName());
 * mapStoreConfig.setProperty(GenericMapLoader.DATA_CONNECTION_REF_PROPERTY, "mysql-ref");
 * mapConfig.setMapStoreConfig(mapStoreConfig);
 * instance().getConfig().addMapConfig(mapConfig);}</pre>
 * <p>
 * The GenericMapLoader creates a SQL mapping with name "__map-store." + mapName.
 * This mapping is removed when the map is destroyed.
 *
 * @param <K> type of the key
 * @param <V> type of the value
 */
public class GenericMapLoader<K, V> implements MapLoader<K, V>, MapLoaderLifecycleSupport {

    /**
     * Property key to define data connection
     */
    public static final String DATA_CONNECTION_REF_PROPERTY = "data-connection-ref";
    /**
     * Property key to define external name of the table
     */
    public static final String EXTERNAL_NAME_PROPERTY = "external-name";

    /**
     * Property key to define id column name in database
     */
    public static final String ID_COLUMN_PROPERTY = "id-column";

    /**
     * Property key to define column names in database
     */
    public static final String COLUMNS_PROPERTY = "columns";

    /**
     * Property key to data connection type name
     */
    public static final String TYPE_NAME_PROPERTY = "type-name";

    /**
     * Property key to control loading of all keys when IMap is first created
     */
    public static final String LOAD_ALL_KEYS_PROPERTY = "load-all-keys";

    /**
     * Property key to decide on getting a single column as the value
     */
    public static final String SINGLE_COLUMN_AS_VALUE = "single-column-as-value";

    /**
     * Timeout for initialization of GenericMapLoader
     */
    public static final HazelcastProperty MAPSTORE_INIT_TIMEOUT
            = new HazelcastProperty("hazelcast.mapstore.init.timeout", 30, SECONDS);

    static final String MAPPING_PREFIX = "__map-store.";

    protected SqlService sqlService;

    protected GenericMapStoreProperties genericMapStoreProperties;

    protected Queries queries;

    protected List<SqlColumnMetadata> columnMetadataList;

    private ILogger logger;

    private HazelcastInstanceImpl instance;

    private MappingHelper mappingHelper;

    private String mapName;
    private String mappingName;

    private long initTimeoutMillis;

    private Exception initFailure; // uses initFinished latch to ensure visibility

    private final CountDownLatch initFinished = new CountDownLatch(1);

    @Override
    public void init(HazelcastInstance instance, Properties properties, String mapName) {
        validateMapStoreConfig(instance, mapName);

        logger = instance.getLoggingService().getLogger(GenericMapLoader.class);

        this.instance = Util.getHazelcastInstanceImpl(instance);
        this.genericMapStoreProperties = new GenericMapStoreProperties(properties, mapName);
        this.sqlService = instance.getSql();
        this.mappingHelper = new MappingHelper(this.sqlService);

        this.mapName = mapName;
        this.mappingName = MAPPING_PREFIX + mapName;

        HazelcastProperties hzProperties = nodeEngine().getProperties();
        this.initTimeoutMillis = hzProperties.getMillis(MAPSTORE_INIT_TIMEOUT);

        ManagedExecutorService asyncExecutor = getMapStoreExecutor();

        // Init can run on partition thread, creating a mapping uses other maps, so it needs to run elsewhere
        asyncExecutor.submit(this::createOrReadMapping);
    }

    private void validateMapStoreConfig(HazelcastInstance instance, String mapName) {
        MapConfig mapConfig = instance.getConfig().findMapConfig(mapName);
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        if (!mapStoreConfig.isOffload()) {
            throw new HazelcastException("MapStoreConfig for " + mapConfig.getName() +
                                         " must have `offload` property set to true");
        }
        if (mapStoreConfig.getProperty(DATA_CONNECTION_REF_PROPERTY) == null) {
            throw new HazelcastException("MapStoreConfig for " + mapConfig.getName() +
                                         " must have `" + DATA_CONNECTION_REF_PROPERTY + "` property set");
        }

        // Validate that property is not an invalid boolean string
        String loadAllKeys = mapStoreConfig.getProperty(LOAD_ALL_KEYS_PROPERTY);
        if (loadAllKeys != null) {
            if (!isBoolean(loadAllKeys)) {
                throw new HazelcastException("MapStoreConfig for " + mapConfig.getName() +
                                             " must have `" + LOAD_ALL_KEYS_PROPERTY + "` property set as true or false");
            }
        }
    }

   private ManagedExecutorService getMapStoreExecutor() {
        return nodeEngine()
                .getExecutionService()
                .getExecutor(ExecutionService.MAP_STORE_OFFLOADABLE_EXECUTOR);
    }

    private NodeEngineImpl nodeEngine() {
        return instance.node.nodeEngine;
    }

    private void createOrReadMapping() {
        logger.fine("Initializing for map " + mapName);
        try {
            List<SqlColumnMetadata> mappingColumns = null;
            if (genericMapStoreProperties.hasColumns()) {
                mappingColumns = resolveMappingColumns();
                logger.fine("Discovered following mapping columns: " + mappingColumns);
            }

            mappingHelper.createMapping(
                    mappingName,
                    genericMapStoreProperties.tableName,
                    mappingColumns,
                    genericMapStoreProperties.dataConnectionRef,
                    genericMapStoreProperties.idColumn
            );

            if (!genericMapStoreProperties.hasColumns()) {
                columnMetadataList = mappingHelper.loadColumnMetadataFromMapping(mappingName);
            }
            queries = new Queries(mappingName, genericMapStoreProperties.idColumn, columnMetadataList);
        } catch (Exception e) {
            // We create the mapping on the first member initializing this object
            // Other members trying to concurrently initialize will fail and just read the mapping
            if (e.getMessage() != null && e.getMessage().startsWith("Mapping or view already exists:")) {
                readExistingMapping();
            } else {
                logger.severe(e);
                initFailure = e;
            }
        } finally {
            initFinished.countDown();
        }
    }

    private List<SqlColumnMetadata> resolveMappingColumns() {
        // Create a temporary mapping
        String tempMapping = "temp_mapping_" + UuidUtil.newUnsecureUuidString();
        mappingHelper.createMapping(
                tempMapping,
                genericMapStoreProperties.tableName,
                null,
                genericMapStoreProperties.dataConnectionRef,
                genericMapStoreProperties.idColumn
        );

        columnMetadataList = mappingHelper.loadColumnMetadataFromMapping(tempMapping);
        Map<String, SqlColumnMetadata> columnMap = columnMetadataList
                .stream()
                .collect(toMap(SqlColumnMetadata::getName, identity()));
        dropMapping(tempMapping);

        return genericMapStoreProperties.getAllColumns().stream()
                                        .map(columnName -> validateColumn(columnMap, columnName))
                                        .collect(Collectors.toList());
    }

    private void readExistingMapping() {
        logger.fine("Reading existing mapping for map " + mapName);
        try {
            // If mappingName does not exist, we get "... did you forget to CREATE MAPPING?" exception
            columnMetadataList = mappingHelper.loadColumnMetadataFromMapping(mappingName);
            Map<String, SqlColumnMetadata> columnMap = columnMetadataList
                    .stream()
                    .collect(toMap(SqlColumnMetadata::getName, identity()));
            validateColumnsExist(columnMap, genericMapStoreProperties.getAllColumns());
            queries = new Queries(mappingName, genericMapStoreProperties.idColumn, columnMetadataList);

        } catch (Exception e) {
            initFailure = e;
        }
    }

    @Override
    public void destroy() {
        ManagedExecutorService asyncExecutor = getMapStoreExecutor();

        asyncExecutor.submit(() -> {
            awaitInitFinished();
            // Instance is not shutting down.
            // Only GenericMapLoader is being closed
            if (instance.isRunning()) {
                dropMapping(mappingName);
            }
        });
    }

    private void dropMapping(String mappingName) {
        logger.info("Dropping mapping " + mappingName);
        try {
            mappingHelper.dropMapping(mappingName);
        } catch (Exception e) {
            logger.warning("Failed to drop mapping " + mappingName, e);
        }
    }

    @Override
    public V load(K key) {
        awaitSuccessfulInit();

        try (SqlResult queryResult = sqlService.execute(queries.load(), key)) {
            Iterator<SqlRow> it = queryResult.iterator();

            V value = null;
            if (it.hasNext()) {
                SqlRow sqlRow = it.next();
                if (it.hasNext()) {
                    throw new IllegalStateException("multiple matching rows for a key " + key);
                }
                // If there is a single column as the value, return that column as the value
                if (queryResult.getRowMetadata().getColumnCount() == 2 && genericMapStoreProperties.singleColumnAsValue) {
                    value = sqlRow.getObject(1);
                } else {
                    //noinspection unchecked
                    value = (V) toGenericRecord(sqlRow, genericMapStoreProperties);
                }
            }
            return value;
        }
    }

    /**
     * Size of the {@code keys} collection is limited by {@link ClusterProperty#MAP_LOAD_CHUNK_SIZE}
     */
    @Override
    public Map<K, V> loadAll(Collection<K> keys) {
        awaitSuccessfulInit();

        Object[] keysArray = keys.toArray();

        String sql = queries.loadAll(keys.size());
        try (SqlResult queryResult = sqlService.execute(sql, keysArray)) {
            Iterator<SqlRow> it = queryResult.iterator();

            Map<K, V> result = new HashMap<>();

            while (it.hasNext()) {
                SqlRow sqlRow = it.next();
                // If there is a single column as the value, return that column as the value
                if (queryResult.getRowMetadata().getColumnCount() == 2 && genericMapStoreProperties.singleColumnAsValue) {
                    K id = sqlRow.getObject(genericMapStoreProperties.idColumn);
                    result.put(id, sqlRow.getObject(1));
                } else {
                    K id = sqlRow.getObject(genericMapStoreProperties.idColumn);
                    //noinspection unchecked
                    V record = (V) toGenericRecord(sqlRow, genericMapStoreProperties);
                    result.put(id, record);
                }
            }
            return result;
        }
    }

    @Override
    public Iterable<K> loadAllKeys() {
        // If loadAllKeys property is disabled, don't load anything
        if (!genericMapStoreProperties.loadAllKeys) {
            return Collections.emptyList();
        }

        awaitSuccessfulInit();

        String sql = queries.loadAllKeys();
        SqlResult keysResult = sqlService.execute(sql);

        // The contract for loadAllKeys says that if iterator implements Closable
        // then it will be closed when the iteration is over
        return () -> new MappingClosingIterator<>(
                keysResult.iterator(),
                (SqlRow row) -> row.getObject(genericMapStoreProperties.idColumn),
                keysResult::close
        );
    }

    /**
     * Awaits successful initialization; if the initialization fails, throws an exception.
     */
    protected void awaitSuccessfulInit() {
        awaitInitFinished();
        if (initFailure != null) {
            throw new HazelcastException("MapStore init failed for map: " + mapName, initFailure);
        }
    }

    void awaitInitFinished() {
        try {
            boolean finished = initFinished.await(initTimeoutMillis, MILLISECONDS);
            if (!finished) {
                throw new HazelcastException("MapStore init for map: " + mapName + " timed out after "
                        + initTimeoutMillis + " ms", initFailure);
            }
        } catch (InterruptedException e) {
            throw new HazelcastException(e);
        }
    }

    // Visible for testing
    boolean initHasFinished() {
        return initFinished.getCount() == 0;
    }
}
