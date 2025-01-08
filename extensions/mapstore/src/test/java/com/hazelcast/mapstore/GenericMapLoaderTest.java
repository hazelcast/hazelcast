/*
 * Copyright 2025 Hazelcast Inc.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.impl.compact.CompactGenericRecord;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlTestSupport;
import com.hazelcast.jet.test.SerialTest;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.sql.SqlService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import com.hazelcast.test.jdbc.JdbcDatabaseProvider;
import com.hazelcast.test.jdbc.JdbcObjectProvider;
import com.hazelcast.test.jdbc.MySQLDatabaseProvider;
import com.hazelcast.test.jdbc.TestDatabaseProvider;
import com.hazelcast.test.jdbc.TestDatabaseRecordProvider;
import com.hazelcast.test.jdbc.TestDatabaseRecordProvider.ObjectSpec;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.mapstore.GenericMapLoader.COLUMNS_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.DATA_CONNECTION_REF_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.EXTERNAL_NAME_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.ID_COLUMN_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.LOAD_ALL_KEYS_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.MAPPING_PREFIX;
import static com.hazelcast.mapstore.GenericMapLoader.SINGLE_COLUMN_AS_VALUE;
import static com.hazelcast.mapstore.GenericMapLoader.TYPE_NAME_PROPERTY;
import static com.hazelcast.nio.serialization.FieldKind.NOT_AVAILABLE;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static com.hazelcast.test.jdbc.TestDatabaseRecordProvider.Column.col;
import static com.hazelcast.test.jdbc.TestDatabaseRecordProvider.ColumnType.INT;
import static com.hazelcast.test.jdbc.TestDatabaseRecordProvider.ColumnType.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Lists.newArrayList;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/**
 * This test runs the MapLoader methods directly, but it runs within real Hazelcast instance
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, SerialTest.class})
public class GenericMapLoaderTest extends SqlTestSupport {

    protected static final String TEST_DATABASE_REF = "testDatabaseRef";

    protected static TestDatabaseProvider databaseProvider;
    protected static TestDatabaseRecordProvider objectProvider;

    protected static String dbConnectionUrl;
    protected static SqlService sqlService;

    protected String mapName;

    protected HazelcastInstance hz;
    private GenericMapLoader<Integer, GenericRecord> mapLoader;
    private GenericMapLoader<Integer, String> mapLoaderSingleColumn;

    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider());
    }

    @BeforeClass
    public static void checkDockerEnabled() {
        assumeDockerEnabled();
    }

    public static <TDP extends TestDatabaseProvider> void initialize(TDP databaseProvider) {
        initialize(databaseProvider, smallInstanceConfig());
    }

    public static <TDP extends TestDatabaseProvider> void initialize(TDP newDatabaseProvider,
                                  Config config) {
        databaseProvider = newDatabaseProvider;
        objectProvider = databaseProvider.recordProvider();
        dbConnectionUrl = databaseProvider.createDatabase(JdbcSqlTestSupport.class.getSimpleName());
        config.addDataConnectionConfig(
                new DataConnectionConfig(TEST_DATABASE_REF)
                        .setType(GenericMapLoaderTest.databaseProvider.dataConnectionType())
                        .setProperties(GenericMapLoaderTest.databaseProvider.properties())
        );
        initialize(2, config);
        sqlService = instance().getSql();
    }

    @Before
    public void setUp() {
        hz = instances()[0];
        mapName = "people_" + randomName();
    }

    @After
    public void after() {
        if (mapLoader != null && mapLoader.initHasFinished()) {
            mapLoader.destroy();
            mapLoader = null;
        }
        if (mapLoaderSingleColumn != null && mapLoaderSingleColumn.initHasFinished()) {
            mapLoaderSingleColumn.destroy();
            mapLoaderSingleColumn = null;
        }
    }

    @Test
    public void whenMapLoaderInit_thenCreateMappingForMapStoreConfig() {
        objectProvider.createObject(mapName, false);

        mapLoader = createMapLoader();
        assertMappingCreated();
    }

    @Test
    public void whenMapLoaderInitCalledOnNonMaster_thenInitAndLoadValue() {
        ObjectSpec spec = objectProvider.createObject(mapName, false);
        objectProvider.insertItems(spec, 1);

        mapLoader = createMapLoader(instances()[1]);
        GenericRecord genericRecord = mapLoader.load(0);
        assertThat(genericRecord).isNotNull();
    }

    protected static void createMapping(String tableName, String mappingName) {
        sqlService.executeUpdate(
                "CREATE MAPPING \"" + mappingName + "\""
                        + " EXTERNAL NAME " + tableName + " "
                        + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );
    }

    @Test
    public void givenValidMappingExists_whenMapLoaderInit_thenInitAndLoadRecord() {
        ObjectSpec spec = objectProvider.createObject(mapName, false);
        objectProvider.insertItems(spec, 1);
        createMapping(mapName, MAPPING_PREFIX + mapName);

        mapLoader = createMapLoader();
        GenericRecord loaded = mapLoader.load(0);
        assertThat(loaded).isNotNull();
    }

    @Test
    public void whenMapLoaderDestroyOnMaster_thenDropMapping() {
        objectProvider.createObject(mapName, false);

        mapLoader = createMapLoader();
        assertMappingCreated();

        mapLoader.destroy();
        assertMappingDestroyed();
    }

    @Test
    public void whenMapLoaderDestroyOnNonMaster_thenDropMapping() {
        objectProvider.createObject(mapName, false);

        mapLoader = createMapLoader();
        assertMappingCreated();

        GenericMapLoader<Object, GenericRecord> mapLoaderNotMaster = createMapLoader(instances()[1]);
        mapLoaderNotMaster.destroy();
        assertMappingDestroyed();
    }

    @Test
    public void whenMapLoaderInitOnNonMaster_thenLoadWaitsForSuccessfulInit() {
        ObjectSpec spec = objectProvider.createObject(mapName, false);
        objectProvider.insertItems(spec, 1);

        GenericMapLoader<Object, GenericRecord> mapLoaderNonMaster = createMapLoader(instances()[1]);
        mapLoader = createMapLoader();

        GenericRecord genericRecord = mapLoaderNonMaster.load(0);
        assertThat(genericRecord).isNotNull();
    }

    @Test
    public void givenRow_whenLoad_thenReturnGenericRecord() {
        ObjectSpec spec = objectProvider.createObject(mapName, false);
        objectProvider.insertItems(spec, 1);

        mapLoader = createMapLoader();
        GenericRecord genericRecord = mapLoader.load(0);

        assertThat(genericRecord.getInt32("id")).isZero();
        assertThat(genericRecord.getString("name")).isEqualTo("name-0");
    }

    @Test
    public void givenRow_whenLoad_thenReturnSingleColumn() {
        ObjectSpec spec = objectProvider.createObject(mapName);
        objectProvider.insertItems(spec, 1);

        mapLoaderSingleColumn = createMapLoaderSingleColumn();
        String name = mapLoaderSingleColumn.load(0);

        assertThat(name).isEqualTo("name-0");
    }

    @Test
    public void givenTableMultipleColumns_whenLoad_thenReturnGenericRecord() {
        var spec = new ObjectSpec(mapName,
                col("id", INT),
                col("name", STRING),
                col("age", INT),
                col("address", STRING));
        objectProvider.createObject(spec);
        objectProvider.insertItems(spec, 1);

        mapLoader = createMapLoader();
        GenericRecord genericRecord = mapLoader.load(0);

        assertThat(genericRecord.getInt32("id")).isZero();
        assertThat(genericRecord.getString("name")).isEqualTo("name-0");
        assertThat(genericRecord.getInt32("age")).isEqualTo(2);
        assertThat(genericRecord.getString("address")).isEqualTo("address-0");
    }

    @Test
    public void givenTableVarcharPKColumn_whenLoad_thenReturnGenericRecordWithCorrectType() {
        var spec = new ObjectSpec(mapName, col("id", STRING), col("name", STRING));
        objectProvider.createObject(spec);
        objectProvider.insertItems(spec, 1);

        GenericMapLoader<String, GenericRecord> mapLoader = createMapLoader();
        GenericRecord genericRecord = mapLoader.load("id-0");

        assertThat(genericRecord.getString("id")).isEqualTo("id-0");
        assertThat(genericRecord.getString("name")).isEqualTo("name-0");
        mapLoader.destroy();
    }

    @Test
    public void givenTableVarcharPKColumn_whenLoad_thenReturnSingleColumnWithCorrectType() {
        var spec = new ObjectSpec(mapName,
                col("id", STRING),
                col("name", STRING)
        );
        objectProvider.createObject(spec);
        objectProvider.insertItems(spec, 1);

        GenericMapLoader<String, String> mapLoader = createMapLoaderSingleColumn();
        String name = mapLoader.load("id-0");

        assertThat(name).isEqualTo("name-0");

        mapLoader.destroy();
    }

    @Test
    public void givenTable_whenSetColumns_thenGenericRecordHasSetColumns() {
        ObjectSpec spec = new ObjectSpec(mapName,
                col("id", INT),
                col("name", STRING),
                col("age", INT),
                col("address", STRING));
        objectProvider.createObject(spec);
        objectProvider.insertItems(spec, 1);

        Properties properties =
                new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(COLUMNS_PROPERTY, "id,name,age");
        mapLoader = createMapLoader(properties, hz);

        GenericRecord genericRecord = mapLoader.load(0);

        assertThat(genericRecord.getInt32("id")).isZero();
        assertThat(genericRecord.getString("name")).isEqualTo("name-0");
        assertThat(genericRecord.getInt32("age")).isEqualTo(2);
        assertThat(genericRecord.getFieldKind("address")).isEqualTo(NOT_AVAILABLE);
    }

    @Test
    public void whenSetNonExistingColumnOnSecondMapStore_thenFailToInitialize() {
        ObjectSpec spec = new ObjectSpec(mapName,
                col("id", INT),
                col("name", STRING),
                col("age", INT));
        objectProvider.createObject(spec);
        objectProvider.insertItems(spec, 1);
        createMapping(mapName, MAPPING_PREFIX + mapName);
        assertMappingCreated();
        // This simulates a second map store on a different instance. The mapping is created, but must be validated
        // (e.g. the config might differ on members)
        Properties secondProps = new Properties();
        secondProps.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        secondProps.setProperty(COLUMNS_PROPERTY, "id,name,age");
        mapLoader = createUnitUnderTest(secondProps, hz, false);
        mapLoader.init(hz, secondProps, mapName);

        assertThatThrownBy(() -> mapLoader.load(0))
                .isInstanceOf(HazelcastException.class)
                .hasStackTraceContaining("Column 'age' not found");
    }

    @Test
    public void whenSetNonExistingColumn_thenFailToInitialize() {
        ObjectSpec spec = objectProvider.createObject(mapName, false);
        objectProvider.insertItems(spec, 1);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);

        properties.setProperty("columns", "name,age");
        mapLoader = createMapLoader(properties, hz);

        assertThatThrownBy(() -> mapLoader.load(0))
                .isInstanceOf(HazelcastException.class)
                .hasStackTraceContaining("Column 'age' not found");
    }

    @Test
    public void whenSetNonExistingColumnOnSecondMapLoader_thenFailToInitialize() {
        var spec = new ObjectSpec(mapName,
                col("id", INT),
                col("name", STRING),
                col("age", INT));
        objectProvider.createObject(spec);
        objectProvider.insertItems(spec, 1);
        createMapping(mapName, MAPPING_PREFIX + mapName);
        assertMappingCreated();
        // This simulates a second map loader on a different instance. The mapping is created, but must be validated
        // (e.g. the config might differ on members)
        Properties secondProps = new Properties();
        secondProps.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        secondProps.setProperty(COLUMNS_PROPERTY, "id,name,age");
        mapLoader = createUnitUnderTest(secondProps, hz, false);
        mapLoader.init(hz, secondProps, mapName);

        assertThatThrownBy(() -> mapLoader.load(0))
                .isInstanceOf(HazelcastException.class)
                .hasStackTraceContaining("Column 'age' not found");
    }

    @Test
    public void givenDefaultTypeName_whenLoad_thenReturnGenericRecordMapNameAsTypeName() {
        ObjectSpec spec = objectProvider.createObject(mapName, false);
        objectProvider.insertItems(spec, 1);

        mapLoader = createMapLoader();

        CompactGenericRecord genericRecord = (CompactGenericRecord) mapLoader.load(0);
        assertThat(genericRecord.getSchema().getTypeName()).isEqualTo(mapName);
    }

    @Test
    public void givenTypeName_whenLoad_thenReturnGenericRecordWithCorrectTypeName() {
        ObjectSpec spec = objectProvider.createObject(mapName, false);
        objectProvider.insertItems(spec, 1);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(TYPE_NAME_PROPERTY, "my.Person");
        mapLoader = createMapLoader(properties, hz);

        CompactGenericRecord genericRecord = (CompactGenericRecord) mapLoader.load(0);
        assertThat(genericRecord.getSchema().getTypeName()).isEqualTo("my.Person");
    }

    @Test
    public void givenRowAndIdColumn_whenLoad_thenReturnGenericRecord() {
        ObjectSpec spec = objectProvider.createObject(mapName, true);
        objectProvider.insertItems(spec, 1);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        mapLoader = createMapLoader(properties, hz);
        GenericRecord genericRecord = mapLoader.load(0);

        assertThat(genericRecord.getInt32("person-id")).isZero();
        assertThat(genericRecord.getString("name")).isEqualTo("name-0");
    }

    @Test
    public void givenRowAndIdColumn_whenLoad_thenReturnSingleColumn() {
        ObjectSpec spec = objectProvider.createObject(mapName, true);
        objectProvider.insertItems(spec, 1);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        properties.setProperty(SINGLE_COLUMN_AS_VALUE, "true");
        mapLoaderSingleColumn = createMapLoader(properties, hz);
        String name = mapLoaderSingleColumn.load(0);

        assertThat(name).isEqualTo("name-0");
    }

    @Test
    public void givenRowAndColumnsWithoutId_whenLoadAndLoadAll_thenReturnGenericRecordWithoutId() {
        ObjectSpec spec = objectProvider.createObject(mapName, false);
        objectProvider.insertItems(spec, 1);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(COLUMNS_PROPERTY, "name");
        mapLoader = createMapLoader(properties, hz);

        GenericRecord genericRecord = mapLoader.load(0);
        assertThat(genericRecord.getFieldKind("id")).isEqualTo(NOT_AVAILABLE);

        Map<Integer, GenericRecord> records = mapLoader.loadAll(newArrayList(0));
        assertThat(records.get(0).getFieldKind("id")).isEqualTo(NOT_AVAILABLE);
    }

    @Test
    public void givenRowDoesNotExist_whenLoad_thenReturnNull() {
        objectProvider.createObject(mapName, false);
        mapLoader = createMapLoader();

        GenericRecord genericRecord = mapLoader.load(0);
        assertThat(genericRecord).isNull();
    }

    @Test
    public void givenRowDoesNotExist_whenLoad_thenReturnNullSingleColumn() {
        objectProvider.createObject(mapName);
        mapLoaderSingleColumn = createMapLoaderSingleColumn();

        String name = mapLoaderSingleColumn.load(0);
        assertThat(name).isNull();
    }

    @Test
    public void givenRow_whenLoadAll_thenReturnMapWithGenericRecord() {
        ObjectSpec spec = objectProvider.createObject(mapName, false);
        mapLoader = createMapLoader();

        objectProvider.insertItems(spec, 1);

        Map<Integer, GenericRecord> records = mapLoader.loadAll(newArrayList(0));

        assertThat(records).containsKey(0);

        GenericRecord genericRecord = records.values().iterator().next();
        records.values().iterator().next();
        assertThat(genericRecord.getInt32("id")).isZero();
        assertThat(genericRecord.getString("name")).isEqualTo("name-0");
    }

    @Test
    public void givenRow_whenLoadAll_thenReturnMapWithSingleColumn() {
        ObjectSpec spec = objectProvider.createObject(mapName);
        mapLoaderSingleColumn = createMapLoaderSingleColumn();

        objectProvider.insertItems(spec, 1);

        Map<Integer, String> names = mapLoaderSingleColumn.loadAll(newArrayList(0));

        assertThat(names).containsKey(0);

        String name = names.values().iterator().next();
        names.values().iterator().next();
        assertThat(name).isEqualTo("name-0");
    }

    @Test
    public void givenRowAndIdColumn_whenLoadAll_thenReturnGenericRecord() {
        ObjectSpec spec = objectProvider.createObject(mapName, true);
        objectProvider.insertItems(spec, 1);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        mapLoader = createMapLoader(properties, hz);
        GenericRecord genericRecord = mapLoader.loadAll(newArrayList(0)).get(0);

        assertThat(genericRecord.getInt32("person-id")).isZero();
        assertThat(genericRecord.getString("name")).isEqualTo("name-0");
    }

    @Test
    public void givenRowAndIdColumn_whenLoadAll_thenReturnSingleColumn() {
        ObjectSpec spec = objectProvider.createObject(mapName, true);
        objectProvider.insertItems(spec, 1);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        properties.setProperty(SINGLE_COLUMN_AS_VALUE, "true");
        mapLoaderSingleColumn = createMapLoader(properties, hz);
        String name = mapLoaderSingleColumn.loadAll(newArrayList(0)).get(0);

        assertThat(name).isEqualTo("name-0");
    }

    @Test
    public void givenRowAndIdColumn_whenLoadAllMultipleItems_thenReturnGenericRecords() {
        ObjectSpec spec = objectProvider.createObject(mapName, true);
        objectProvider.insertItems(spec, 2);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        mapLoader = createMapLoader(properties, hz);
        Map<Integer, GenericRecord> records = mapLoader.loadAll(newArrayList(0, 1));

        assertThat(records).hasSize(2);
    }

    @Test
    public void givenRowAndIdColumn_whenLoadAllMultipleItems_thenReturnSingleColumn() {
        ObjectSpec spec = objectProvider.createObject(mapName, true);
        objectProvider.insertItems(spec, 2);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        properties.setProperty(SINGLE_COLUMN_AS_VALUE, "true");
        mapLoaderSingleColumn = createMapLoader(properties, hz);
        Map<Integer, String> names = mapLoaderSingleColumn.loadAll(newArrayList(0, 1));

        assertThat(names).hasSize(2);

    }

    @Test
    public void givenRowDoesNotExist_whenLoadAll_thenReturnEmptyMap() {
        objectProvider.createObject(mapName, false);
        mapLoader = createMapLoader();

        Map<Integer, GenericRecord> records = mapLoader.loadAll(newArrayList(0));
        assertThat(records).isEmpty();
    }

    @Test
    public void givenRowDoesNotExist_whenLoadAllWithSingleColumn_thenReturnEmptyMap() {
        objectProvider.createObject(mapName);
        mapLoaderSingleColumn = createMapLoaderSingleColumn();

        Map<Integer, String> names = mapLoaderSingleColumn.loadAll(newArrayList(0));
        assertThat(names).isEmpty();
    }

    @Test
    public void givenRow_whenLoadAllKeys_thenReturnKeys() {
        ObjectSpec spec = objectProvider.createObject(mapName, false);
        mapLoader = createMapLoader();

        objectProvider.insertItems(spec, 1);

        List<Integer> ids = newArrayList(mapLoader.loadAllKeys());
        assertThat(ids).contains(0);
    }

    @Test
    public void givenRow_whenLoadAllKeysWithSingleColumn_thenReturnKeys() {
        ObjectSpec spec = objectProvider.createObject(mapName);
        mapLoaderSingleColumn = createMapLoaderSingleColumn();

        objectProvider.insertItems(spec, 1);

        List<Integer> ids = newArrayList(mapLoaderSingleColumn.loadAllKeys());
        assertThat(ids).contains(0);
    }

    @Test
    public void givenRowAndIdColumn_whenLoadAllKeys_thenReturnKeys() {
        ObjectSpec spec = objectProvider.createObject(mapName, true);
        objectProvider.insertItems(spec, 1);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);

        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        mapLoader = createMapLoader(properties, hz);

        List<Integer> ids = newArrayList(mapLoader.loadAllKeys());
        assertThat(ids).contains(0);
    }

    @Test
    public void givenRowAndIdColumn_whenLoadAllKeysWithSingleColumn_thenReturnKeys() {
        ObjectSpec spec = objectProvider.createObject(mapName, true);
        objectProvider.insertItems(spec, 1);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(SINGLE_COLUMN_AS_VALUE, "true");
        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        mapLoaderSingleColumn = createMapLoader(properties, hz);

        List<Integer> ids = newArrayList(mapLoaderSingleColumn.loadAllKeys());
        assertThat(ids).contains(0);
    }

    @Test
    public void givenFalse_whenLoadAllKeys_thenReturnNull() {
        ObjectSpec spec = objectProvider.createObject(mapName, true);
        objectProvider.insertItems(spec, 1);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);

        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        properties.setProperty(LOAD_ALL_KEYS_PROPERTY, "false");
        mapLoader = createMapLoader(properties, hz);

        List<Integer> ids = newArrayList(mapLoader.loadAllKeys());
        assertThat(ids).isEmpty();
    }

    @Test
    public void givenTrue_whenLoadAllKeys_thenReturnKeys() {
        ObjectSpec spec = objectProvider.createObject(mapName, true);
        objectProvider.insertItems(spec, 1);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);

        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        properties.setProperty(LOAD_ALL_KEYS_PROPERTY, "true");
        mapLoader = createMapLoader(properties, hz);

        List<Integer> ids = newArrayList(mapLoader.loadAllKeys());
        assertThat(ids).contains(0);
    }

    @Test
    public void givenInvalid_whenLoadAllKeys_thenReturnKeys() {
        ObjectSpec spec = objectProvider.createObject(mapName, true);
        objectProvider.insertItems(spec, 1);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);

        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        properties.setProperty(LOAD_ALL_KEYS_PROPERTY, "invalidBooleanValue");
        assertThatThrownBy(() -> createMapLoader(properties, hz))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("MapStoreConfig for " + mapName + " must have `load-all-keys` property set as true or false");


    }

    @Test
    public void givenNoRows_whenLoadAllKeys_thenEmptyIterable() {
        objectProvider.createObject(mapName, false);
        mapLoader = createMapLoader();

        Iterable<Integer> ids = mapLoader.loadAllKeys();
        assertThat(ids).isEmpty();
    }

    @Test
    public void givenMapStoreConfigWithOffloadDisabled_thenFail() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setClassName(GenericMapLoader.class.getName())
                .setOffload(false);

        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        instance().getConfig().addMapConfig(mapConfig);

        mapLoader = new GenericMapLoader<>();
        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);

        assertThatThrownBy(() -> mapLoader.init(hz, properties, mapName))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("MapStoreConfig for " + mapName + " must have `offload` property set to true");
    }

    @Test
    public void givenMapStoreConfig_WithoutDataConnection_thenFail() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setClassName(GenericMapLoader.class.getName());

        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        instance().getConfig().addMapConfig(mapConfig);

        mapLoader = new GenericMapLoader<>();
        Properties properties = new Properties();

        assertThatThrownBy(() -> mapLoader.init(hz, properties, mapName))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("MapStoreConfig for " + mapName + " must have `data-connection-ref` property set");
    }

    @Test
    public void givenTableNameProperty_whenCreateMapLoader_thenUseTableName() {
        String tableName = randomName();

        ObjectSpec spec = objectProvider.createObject(tableName, false);
        objectProvider.insertItems(spec, 1);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(EXTERNAL_NAME_PROPERTY, tableName);
        mapLoader = createMapLoader(properties, hz);

        GenericRecord genericRecord = mapLoader.load(0);
        assertThat(genericRecord).isNotNull();
    }

    @Test
    public void givenTableNameProperty_whenCreateMapLoader_thenUseTableNameWithCustomSchema() {
        assumeTrue(objectProvider instanceof JdbcDatabaseProvider);
        var jdbcDatabaseProvider = (JdbcObjectProvider) objectProvider;
        String schemaName = "custom_schema";
        jdbcDatabaseProvider.createSchema(schemaName);
        String tableName = randomName() + "-with-hyphen";
        String fullTableName = schemaName + "." + databaseProvider.quote(tableName);

        ObjectSpec spec = objectProvider.createObject(fullTableName, false);
        objectProvider.insertItems(spec, 1);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(EXTERNAL_NAME_PROPERTY, schemaName + ".\"" + tableName + "\"");
        mapLoader = createMapLoader(properties, hz);

        GenericRecord genericRecord = mapLoader.load(0);
        assertThat(genericRecord).isNotNull();
    }

    @Test
    public void givenTableNameProperty_whenCreateMapLoader_thenUseTableNameWithCustomSchemaWithDotInName() {
        assumeTrue(objectProvider instanceof JdbcDatabaseProvider);
        var jdbcDatabaseProvider = (JdbcObjectProvider) objectProvider;
        // See MySQLSchemaJdbcSqlConnectorTest
        assumeFalse(MySQLDatabaseProvider.TEST_MYSQL_VERSION.startsWith("5"));

        String schemaName = "custom_schema2";
        jdbcDatabaseProvider.createSchema(schemaName);
        String tableName = randomName() + ".with_dot";
        String fullTableName = schemaName + "." + databaseProvider.quote(tableName);

        ObjectSpec spec = objectProvider.createObject(fullTableName, false);
        objectProvider.insertItems(spec, 1);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(EXTERNAL_NAME_PROPERTY, schemaName + ".\"" + tableName + "\"");
        mapLoader = createMapLoader(properties, hz);

        GenericRecord genericRecord = mapLoader.load(0);
        assertThat(genericRecord).isNotNull();
    }

    private <K, V> GenericMapLoader<K, V> createMapLoader() {
        return createMapLoader(hz);
    }

    private <K, V> GenericMapLoader<K, V> createMapLoaderSingleColumn() {
        return createMapLoaderSingleColumn(hz);
    }

    private <K, V> GenericMapLoader<K, V> createMapLoader(HazelcastInstance instance) {
        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        return createMapLoader(properties, instance);
    }

    private <K, V> GenericMapLoader<K, V> createMapLoaderSingleColumn(HazelcastInstance instance) {
        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(SINGLE_COLUMN_AS_VALUE, "true");
        return createMapLoader(properties, instance);
    }

    private <K, V> GenericMapLoader<K, V> createMapLoader(Properties properties, HazelcastInstance instance) {
        return createUnitUnderTest(properties, instance, true);
    }

    protected <K, V> GenericMapLoader<K, V> createUnitUnderTest(Properties properties, HazelcastInstance instance,
                                                          boolean init) {
        MapConfig mapConfig = createMapConfigWithMapStore(mapName, properties);
        instance.getConfig().addMapConfig(mapConfig);

        GenericMapLoader<K, V> mapLoader = new GenericMapLoader<>();
        if (init) {
            mapLoader.init(instance, properties, mapName);
            mapLoader.awaitInitFinished();
        }
        return mapLoader;
    }

    private MapConfig createMapConfigWithMapStore(String mapName, Properties properties) {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setClassName(GenericMapLoader.class.getName());
        mapStoreConfig.setProperties(properties);
        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        return mapConfig;
    }

    protected void assertMappingCreated() {
        assertTrueEventually(() ->
                assertRowsAnyOrder(hz, "SHOW MAPPINGS", newArrayList(new Row(MAPPING_PREFIX + mapName))), 60);
    }

    protected void assertMappingDestroyed() {
        assertTrueEventually(() -> assertRowsAnyOrder(hz, "SHOW MAPPINGS", newArrayList()), 60);
    }
}
