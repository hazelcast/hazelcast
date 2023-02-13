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
import com.hazelcast.datalink.DataLinkFactory;
import com.hazelcast.datalink.JdbcDataLinkFactory;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.internal.util.executor.ManagedExecutorService;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
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

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlConnector.OPTION_DATA_LINK_REF;
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
 * First define data link, e.g. for JDBC use {@link JdbcDataLinkFactory}:
 * <pre>{@code Config config = new Config();
 * config.addDataLinkConfig(
 *   new DataLinkConfig("mysql-ref")
 *     .setClassName(JdbcDataLinkFactory.class.getName())
 *     .setProperty("jdbcUrl", dbConnectionUrl)
 * );}</pre>
 * <p>
 * Then create a Map with {@link MapStore} using the GenericMapStore implementation:
 * <pre>{@code MapConfig mapConfig = new MapConfig(mapName);
 * MapStoreConfig mapStoreConfig = new MapStoreConfig();
 * mapStoreConfig.setClassName(GenericMapStore.class.getName());
 * mapStoreConfig.setProperty(OPTION_DATA_LINK_REF, "mysql-ref");
 * mapConfig.setMapStoreConfig(mapStoreConfig);
 * instance().getConfig().addMapConfig(mapConfig);}</pre>
 * <p>
 * The GenericMapStore creates a SQL mapping with name "__map-store." + mapName.
 * This mapping is removed when the map is destroyed.
 * <p>
 * Note : When GenericMapStore uses GenericRecord as value, even if the GenericRecord contains the primary key as a field,
 * the primary key is still received from @{link {@link com.hazelcast.map.IMap} method call
 *
 * @param <K>
 */
public class GenericMapStore<K> implements MapStore<K, GenericRecord>, MapLoaderLifecycleSupport {

    /**
     * Timeout for initialization of GenericMapStore
     */
    public static final HazelcastProperty MAPSTORE_INIT_TIMEOUT
            = new HazelcastProperty("hazelcast.mapstore.init.timeout", 30, SECONDS);

    static final String MAPPING_PREFIX = "__map-store.";

    static final String DATA_LINK_REF_PROPERTY = "data-link-ref";
    static final String TABLE_NAME_PROPERTY = "table-name";
    static final String MAPPING_TYPE_PROPERTY = "mapping-type";

    static final String ID_COLUMN_PROPERTY = "id-column";
    static final String ID_COLUMN_DEFAULT = "id";

    static final String COLUMNS_PROPERTY = "columns";
    static final String TYPE_NAME_PROPERTY = "type-name";

    static final String MAPPING_NAME_COLUMN = "name";

    private ILogger logger;

    private HazelcastInstanceImpl instance;
    private SqlService sql;

    private GenericMapStoreProperties properties;
    private String mapName;
    private String mapping;
    private Queries queries;

    private long initTimeoutMillis;

    private Exception initFailure; // uses initFinished latch to ensure visibility
    private List<SqlColumnMetadata> columnMetadataList;
    private final CountDownLatch initFinished = new CountDownLatch(1);

    @Override
    public void init(HazelcastInstance instance, Properties properties, String mapName) {
        verifyMapStoreOffload(instance, mapName);

        logger = instance.getLoggingService().getLogger(GenericMapStore.class);

        this.instance = Util.getHazelcastInstanceImpl(instance);
        this.properties = new GenericMapStoreProperties(properties, mapName);
        sql = instance.getSql();

        this.mapName = mapName;
        this.mapping = MAPPING_PREFIX + mapName;

        HazelcastProperties hzProperties = nodeEngine().getProperties();
        this.initTimeoutMillis = hzProperties.getMillis(MAPSTORE_INIT_TIMEOUT);

        ManagedExecutorService asyncExecutor = nodeEngine()
                .getExecutionService()
                .getExecutor(ExecutionService.MAP_STORE_OFFLOADABLE_EXECUTOR);

        // Init can run on partition thread, creating a mapping uses other maps, so it needs to run elsewhere
        asyncExecutor.submit(() -> createMappingForMapStore(mapName));
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
                mappingColumns = resolveMappingColumns(properties.tableName, properties.dataLinkRef);
                logger.fine("Discovered following mapping columns: " + mappingColumns);
            }

            sql.execute(
                    "CREATE MAPPING \"" + mapping + "\" "
                            + "EXTERNAL NAME \"" + properties.tableName + "\" "
                            + (mappingColumns != null ? " ( " + mappingColumns + " ) " : "")
                            + "TYPE " + deriveMappingType() + " "
                            + "OPTIONS ("
                            + "    '" + OPTION_DATA_LINK_REF + "' = '" + properties.dataLinkRef + "' "
                            + ")"
            ).close();

            if (!properties.hasColumns()) {
                columnMetadataList = loadMetadataFromMapping(mapping).getColumns();
            }
            queries = new Queries(mapping, properties.idColumn, columnMetadataList);
        } catch (Exception e) {
            // We create the mapping on the first member initializing the MapStore
            // Other members trying to concurrently initialize will fail and just read the mapping
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
            DataLinkFactory<?> factory = nodeEngine()
                    .getDataLinkService()
                    .getDataLinkFactory(properties.dataLinkRef);

            if (factory instanceof JdbcDataLinkFactory) {
                return "JDBC";
            } else {
                throw new HazelcastException("Unknown DataLinkFactory class " + factory.getClass()
                        + ". Set the mapping type using '" + MAPPING_TYPE_PROPERTY + "' property");
            }
        }
    }

    private String resolveMappingColumns(String tableName, String dataLinkRef) {
        String tempMapping = "temp_mapping_" + UuidUtil.newUnsecureUuidString();
        createMapping(tempMapping, tableName, dataLinkRef);
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

    private void createMapping(String mappingName, String tableName, String dataLinkRef) {
        sql.execute(
                "CREATE MAPPING \"" + mappingName + "\""
                        + " EXTERNAL NAME \"" + tableName + "\" "
                        + " TYPE " + deriveMappingType()
                        + " OPTIONS ("
                        + "    '" + OPTION_DATA_LINK_REF + "' = '" + dataLinkRef + "' "
                        + ")"
        ).close();
    }

    private SqlRowMetadata loadMetadataFromMapping(String mapping) {
        try (SqlResult result = sql.execute("SELECT * FROM \"" + mapping + "\" LIMIT 0")) {
            return result.getRowMetadata();
        }
    }

    private void readExistingMapping() {
        logger.fine("Reading existing mapping for map" + mapName);
        try (SqlResult mappings = sql.execute("SHOW MAPPINGS")) {
            for (SqlRow sqlRow : mappings) {
                String name = sqlRow.getObject(MAPPING_NAME_COLUMN);
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
              .forEach(columnName -> validateColumn(rowMetadata.findColumn(columnName), columnName));
    }

    private int validateColumn(int column, String columnName) {
        if (column == COLUMN_NOT_FOUND) {
            throw new HazelcastException("Column '" + columnName + "' not found");
        }
        return column;
    }

    @Override
    public void destroy() {
        ManagedExecutorService asyncExecutor = nodeEngine()
                .getExecutionService()
                .getExecutor(ExecutionService.MAP_STORE_OFFLOADABLE_EXECUTOR);

        asyncExecutor.submit(() -> {
            awaitInitFinished();
            dropMapping(mapping);
        });
    }

    @Override
    public GenericRecord load(K key) {
        awaitSuccessfulInit();

        try (SqlResult queryResult = sql.execute(queries.load(), key)) {
            Iterator<SqlRow> it = queryResult.iterator();
            if (it.hasNext()) {
                SqlRow row = it.next();
                if (it.hasNext()) {
                    throw new IllegalStateException("multiple matching rows for a key " + key);
                }
                return convertRowToGenericRecord(row);
            } else {
                return null;
            }
        }
    }

    @Nonnull
    private GenericRecord convertRowToGenericRecord(SqlRow row) {
        GenericRecordBuilder builder = GenericRecordBuilder.compact(properties.compactTypeName);

        SqlRowMetadata metadata = row.getMetadata();
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            SqlColumnMetadata column = metadata.getColumn(i);

            if (column.getName().equals(properties.idColumn) && !properties.idColumnInColumns) {
                continue;
            }

            switch (column.getType()) {
                case VARCHAR:
                    builder.setString(column.getName(), row.getObject(i));
                    break;

                case BOOLEAN:
                    builder.setBoolean(column.getName(), row.getObject(i));
                    break;

                case TINYINT:
                case SMALLINT:
                case INTEGER:
                    builder.setInt32(column.getName(), row.getObject(i));
                    break;

                case BIGINT:
                    builder.setInt64(column.getName(), row.getObject(i));
                    break;

                case DECIMAL:
                    builder.setDecimal(column.getName(), row.getObject(i));
                    break;

                case REAL:
                    builder.setFloat32(column.getName(), row.getObject(i));
                    break;

                case DOUBLE:
                    builder.setFloat64(column.getName(), row.getObject(i));
                    break;

                case DATE:
                    builder.setDate(column.getName(), row.getObject(i));
                    break;

                case TIME:
                    builder.setTime(column.getName(), row.getObject(i));
                    break;

                case TIMESTAMP:
                    builder.setTimestamp(column.getName(), row.getObject(i));
                    break;

                case TIMESTAMP_WITH_TIME_ZONE:
                    builder.setTimestampWithTimezone(column.getName(), row.getObject(i));
                    break;

                default:
                    throw new HazelcastException("Column type " + column.getType() + " not supported");
            }
        }

        return builder.build();
    }

    /**
     * Size of the {@code keys} collection is limited by {@link ClusterProperty#MAP_LOAD_CHUNK_SIZE}
     */
    @Override
    public Map<K, GenericRecord> loadAll(Collection<K> keys) {
        awaitSuccessfulInit();

        Object[] keysArray = keys.toArray();

        try (SqlResult queryResult = sql.execute(queries.loadAll(keys.size()), keysArray)) {
            Iterator<SqlRow> it = queryResult.iterator();

            Map<K, GenericRecord> result = new HashMap<>();
            while (it.hasNext()) {
                SqlRow row = it.next();
                K id = row.getObject(properties.idColumn);
                GenericRecord record = convertRowToGenericRecord(row);
                result.put(id, record);
            }
            return result;
        }
    }

    @Override
    public Iterable<K> loadAllKeys() {
        awaitSuccessfulInit();

        SqlResult keysResult = sql.execute(queries.loadAllKeys());

        // The contract for loadAllKeys says that if iterator implements Closable
        // then it will be closed when the iteration is over
        return () -> new MappingClosingIterator<>(
                keysResult.iterator(),
                (SqlRow row) -> row.getObject(properties.idColumn),
                keysResult::close
        );
    }

    @Override
    public void store(K key, GenericRecord record) {
        awaitSuccessfulInit();

        JDBCParameters jdbcParameters = GenericRecordUtils.toJDBCParameters(key, record, columnMetadataList,
                properties.idColumn);


        try (SqlResult ignored = sql.execute(queries.storeSink(), jdbcParameters.getParams())) {
        } catch (Exception e) {

            if (isIntegrityConstraintViolation(e)) {

                // Try to update the row
                jdbcParameters.shiftParametersForUpdate();

                String updateSQL = queries.storeUpdate();
                sql.execute(updateSQL, jdbcParameters.getParams()).close();
            } else {
                throw e;
            }
        }
    }

    @Override
    public void storeAll(Map<K, GenericRecord> map) {
        awaitSuccessfulInit();

        for (Entry<K, GenericRecord> entry : map.entrySet()) {
            store(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void delete(K key) {
        awaitSuccessfulInit();

        sql.execute(queries.delete(), key).close();
    }

    @Override
    public void deleteAll(Collection<K> keys) {
        awaitSuccessfulInit();

        if (keys.isEmpty()) {
            return;
        }

        sql.execute(queries.deleteAll(keys.size()), keys.toArray()).close();
    }

    private void dropMapping(String mappingName) {
        sql.execute("DROP MAPPING IF EXISTS \"" + mappingName + "\"").close();
    }

    /**
     * Awaits successful initialization; if the initialization fails, throws an exception.
     */
    private void awaitSuccessfulInit() {
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

    @VisibleForTesting
    boolean initHasFinished() {
        return initFinished.getCount() == 0;
    }

    private static class GenericMapStoreProperties {

        final String dataLinkRef;
        final String tableName;
        final String mappingType;
        final String idColumn;
        final Collection<String> columns;
        final boolean idColumnInColumns;
        final String compactTypeName;

        GenericMapStoreProperties(Properties properties, String mapName) {
            dataLinkRef = properties.getProperty(DATA_LINK_REF_PROPERTY);
            tableName = properties.getProperty(TABLE_NAME_PROPERTY, mapName);
            this.mappingType = properties.getProperty(MAPPING_TYPE_PROPERTY);
            idColumn = properties.getProperty(ID_COLUMN_PROPERTY, ID_COLUMN_DEFAULT);

            String columnsProperty = properties.getProperty(COLUMNS_PROPERTY);
            if (columnsProperty != null) {
                List<String> columnsList = Arrays.asList(columnsProperty.split(","));
                this.columns = Collections.unmodifiableList(columnsList);
            } else {
                columns = Collections.emptyList();
            }
            idColumnInColumns = columns.isEmpty() || columns.contains(idColumn);
            compactTypeName = properties.getProperty(TYPE_NAME_PROPERTY, mapName);
        }

        public boolean hasColumns() {
            return !columns.isEmpty();
        }
    }
}
