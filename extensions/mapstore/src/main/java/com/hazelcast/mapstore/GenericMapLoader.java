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

import com.google.common.annotations.VisibleForTesting;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.datalink.impl.JdbcDataLink;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.internal.util.executor.ManagedExecutorService;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.MapLoaderLifecycleSupport;
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
import com.hazelcast.sql.SqlService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static com.hazelcast.mapstore.ExistingMappingValidator.validateColumn;
import static com.hazelcast.mapstore.ExistingMappingValidator.validateColumnsExist;
import static com.hazelcast.mapstore.FromSqlRowConverter.toGenericRecord;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * GenericMapLoader is an implementation of {@link MapLoader} built
 * on top of Hazelcast SQL engine.
 * <p>
 * It works with any SQL connector supporting SELECT statements.
 * <p>
 * Usage:
 * <p>
 * First define data link, e.g. for JDBC use {@link JdbcDataLink}:
 * <pre>{@code Config config = new Config();
 * config.addDataLinkConfig(
 *   new DataLinkConfig("mysql-ref")
 *     .setClassName(JdbcDataLink.class.getName())
 *     .setProperty("jdbcUrl", dbConnectionUrl)
 * );}</pre>
 * <p>
 * Then create a Map with {@link MapLoader} using the GenericMapLoader implementation:
 * <pre>{@code MapConfig mapConfig = new MapConfig(mapName);
 * MapStoreConfig mapStoreConfig = new MapStoreConfig();
 * mapStoreConfig.setClassName(GenericMapLoader.class.getName());
 * mapStoreConfig.setProperty(JdbcSqlConnector.OPTION_DATA_LINK_REF, "mysql-ref");
 * mapConfig.setMapStoreConfig(mapStoreConfig);
 * instance().getConfig().addMapConfig(mapConfig);}</pre>
 * <p>
 * The GenericMapLoader creates a SQL mapping with name "__map-store." + mapName.
 * This mapping is removed when the map is destroyed.
 *
 * @param <K>
 */
public class GenericMapLoader<K> implements MapLoader<K, GenericRecord>, MapLoaderLifecycleSupport {

    /**
     * Timeout for initialization of GenericMapLoader
     */
    public static final HazelcastProperty MAPSTORE_INIT_TIMEOUT
            = new HazelcastProperty("hazelcast.mapstore.init.timeout", 30, SECONDS);

    static final String MAPPING_PREFIX = "__map-store.";

    static final String DATA_LINK_REF_PROPERTY = "data-link-ref";
    static final String TABLE_NAME_PROPERTY = "table-name";
    static final String MAPPING_TYPE_PROPERTY = "mapping-type";

    static final String ID_COLUMN_PROPERTY = "id-column";

    static final String COLUMNS_PROPERTY = "columns";
    static final String TYPE_NAME_PROPERTY = "type-name";

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
        if (!mapConfig.getMapStoreConfig().isOffload()) {
            throw new HazelcastException("Config for GenericMapStore must have `offload` property set to true");
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
            String mappingColumns = null;
            if (genericMapStoreProperties.hasColumns()) {
                mappingColumns = resolveMappingColumns();
                logger.fine("Discovered following mapping columns: " + mappingColumns);
            }

            mappingHelper.createMappingWithColumns(
                    mappingName,
                    genericMapStoreProperties.tableName,
                    mappingColumns,
                    deriveMappingType(),
                    genericMapStoreProperties.dataLinkRef,
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

    private String deriveMappingType() {
        if (genericMapStoreProperties.mappingType != null) {
            return genericMapStoreProperties.mappingType;
        } else {
            return nodeEngine().getDataLinkService().typeForDataLink(genericMapStoreProperties.dataLinkRef);
        }
    }


    private String resolveMappingColumns() {
        // Create a temporary mapping
        String tempMapping = "temp_mapping_" + UuidUtil.newUnsecureUuidString();
        mappingHelper.createMapping(
                tempMapping,
                genericMapStoreProperties.tableName,
                deriveMappingType(),
                genericMapStoreProperties.dataLinkRef,
                genericMapStoreProperties.idColumn
        );

        SqlRowMetadata rowMetadata = mappingHelper.loadRowMetadataFromMapping(tempMapping);
        columnMetadataList = rowMetadata.getColumns();
        dropMapping(tempMapping);

        return genericMapStoreProperties.getAllColumns().stream()
                .map(columnName -> validateColumn(rowMetadata, columnName))
                .map(rowMetadata::getColumn)
                .map(columnMetadata1 -> columnMetadata1.getName() + " " + columnMetadata1.getType())
                .collect(Collectors.joining(", "));
    }

    private void readExistingMapping() {
        logger.fine("Reading existing mapping for map" + mapName);
        try {
            // If mappingName does not exist, we get "... did you forget to CREATE MAPPING?" exception
            SqlRowMetadata sqlRowMetadata = mappingHelper.loadRowMetadataFromMapping(mappingName);
            validateColumnsExist(sqlRowMetadata, genericMapStoreProperties.getAllColumns());
            columnMetadataList = sqlRowMetadata.getColumns();
            queries = new Queries(mappingName, genericMapStoreProperties.idColumn, columnMetadataList);

        } catch (Exception e) {
            initFailure = e;
        }
    }

    @Override
    public void destroy() {
        ManagedExecutorService asyncExecutor = nodeEngine()
                .getExecutionService()
                .getExecutor(ExecutionService.MAP_STORE_OFFLOADABLE_EXECUTOR);

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
    public GenericRecord load(K key) {
        awaitSuccessfulInit();

        try (SqlResult queryResult = sqlService.execute(queries.load(), key)) {
            Iterator<SqlRow> it = queryResult.iterator();

            GenericRecord genericRecord = null;
            if (it.hasNext()) {
                SqlRow sqlRow = it.next();
                if (it.hasNext()) {
                    throw new IllegalStateException("multiple matching rows for a key " + key);
                }
                genericRecord = toGenericRecord(sqlRow, genericMapStoreProperties);
            }
            return genericRecord;
        }
    }

    /**
     * Size of the {@code keys} collection is limited by {@link ClusterProperty#MAP_LOAD_CHUNK_SIZE}
     */
    @Override
    public Map<K, GenericRecord> loadAll(Collection<K> keys) {
        awaitSuccessfulInit();

        Object[] keysArray = keys.toArray();

        String sql = queries.loadAll(keys.size());
        try (SqlResult queryResult = sqlService.execute(sql, keysArray)) {
            Iterator<SqlRow> it = queryResult.iterator();

            Map<K, GenericRecord> result = new HashMap<>();
            while (it.hasNext()) {
                SqlRow sqlRow = it.next();
                K id = sqlRow.getObject(genericMapStoreProperties.idColumn);
                GenericRecord record = toGenericRecord(sqlRow, genericMapStoreProperties);
                result.put(id, record);
            }
            return result;
        }
    }

    @Override
    public Iterable<K> loadAllKeys() {
        awaitSuccessfulInit();

        String sql = queries.loadAllKeys();
        //noinspection resource
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

    @VisibleForTesting
    boolean initHasFinished() {
        return initFinished.getCount() == 0;
    }
}
