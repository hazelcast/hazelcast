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
import com.hazelcast.internal.serialization.impl.compact.CompactGenericRecord;
import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlTestSupport;
import com.hazelcast.jet.test.SerialTest;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.mapstore.GenericMapStore.COLUMNS_PROPERTY;
import static com.hazelcast.mapstore.GenericMapStore.EXTERNAL_REF_ID_PROPERTY;
import static com.hazelcast.mapstore.GenericMapStore.ID_COLUMN_PROPERTY;
import static com.hazelcast.mapstore.GenericMapStore.MAPPING_PREFIX;
import static com.hazelcast.mapstore.GenericMapStore.TABLE_NAME_PROPERTY;
import static com.hazelcast.mapstore.GenericMapStore.TYPE_NAME_PROPERTY;
import static com.hazelcast.nio.serialization.FieldKind.NOT_AVAILABLE;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Lists.newArrayList;

/**
 * This test runs the MapStore methods directly, but it runs within real Hazelcast instance
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, SerialTest.class})
public class GenericMapStoreTest extends JdbcSqlTestSupport {

    public String mapName;

    private HazelcastInstance hz;
    private GenericMapStore<Integer> mapStore;

    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider());
    }

    @Before
    public void setUp() {
        hz = instances()[0];
        mapName = "people_" + randomName();
    }

    @After
    public void after() {
        if (mapStore != null && mapStore.initHasFinished()) {
            mapStore.destroy();
            mapStore = null;
        }
    }

    @Test
    public void whenMapStoreInit_thenCreateMappingForMapStoreConfig() throws Exception {
        createTable(mapName);

        mapStore = createMapStore();
        assertMappingCreated();
    }

    @Test
    public void whenMapStoreInitCalledOnNonMaster_thenInitAndLoadValue() throws Exception {
        createTable(mapName);
        insertItems(mapName, 1);

        mapStore = createMapStore(instances()[1]);
        GenericRecord record = mapStore.load(0);
        assertThat(record).isNotNull();
    }

    @Test
    public void givenValidMappingExists_whenMapStoreInit_thenInitAndLoadRecord() throws Exception {
        createTable(mapName);
        insertItems(mapName, 1);
        createMapping(mapName, MAPPING_PREFIX + mapName);

        mapStore = createMapStore();
        GenericRecord loaded = mapStore.load(0);
        assertThat(loaded).isNotNull();
    }

    @Test
    public void whenMapStoreDestroyOnMaster_thenDropMapping() throws Exception {
        createTable(mapName);

        mapStore = createMapStore();
        assertMappingCreated();

        mapStore.destroy();
        assertMappingDestroyed();
    }

    @Test
    public void whenMapStoreDestroyOnNonMaster_thenDropMapping() throws Exception {
        createTable(mapName);

        mapStore = createMapStore();
        assertMappingCreated();

        GenericMapStore<Object> mapStoreNotMaster = createMapStore(instances()[1]);
        mapStoreNotMaster.destroy();
        assertMappingDestroyed();
    }

    @Test
    public void whenMapStoreInitOnNonMaster_thenLoadWaitsForSuccessfulInit() throws Exception {
        createTable(mapName);
        insertItems(mapName, 1);

        GenericMapStore<Object> mapStoreNonMaster = createMapStore(instances()[1]);
        mapStore = createMapStore();

        GenericRecord record = mapStoreNonMaster.load(0);
        assertThat(record).isNotNull();
    }

    @Test
    public void givenRow_whenLoad_thenReturnGenericRecord() throws Exception {
        createTable(mapName);
        insertItems(mapName, 1);

        mapStore = createMapStore();
        GenericRecord record = mapStore.load(0);

        assertThat(record.getInt32("id")).isEqualTo(0);
        assertThat(record.getString("name")).isEqualTo("name-0");
    }

    @Test
    public void givenTableMultipleColumns_whenLoad_thenReturnGenericRecord() throws Exception {
        createTable(mapName, "id INT PRIMARY KEY", "name VARCHAR(100)", "age INT", "address VARCHAR(100)");
        executeJdbc("INSERT INTO \"" + mapName + "\" VALUES(0, 'name-0', 42, 'Palo Alto, CA 94306')");

        mapStore = createMapStore();
        GenericRecord record = mapStore.load(0);

        assertThat(record.getInt32("id")).isEqualTo(0);
        assertThat(record.getString("name")).isEqualTo("name-0");
        assertThat(record.getInt32("age")).isEqualTo(42);
        assertThat(record.getString("address")).isEqualTo("Palo Alto, CA 94306");
    }

    @Test
    public void givenTableVarcharPKColumn_whenLoad_thenReturnGenericRecordWithCorrectType() throws Exception {
        createTable(mapName, "id VARCHAR(100)", "name VARCHAR(100)");
        executeJdbc("INSERT INTO \"" + mapName + "\" VALUES('0', 'name-0')");

        GenericMapStore<String> mapStore = createMapStore();
        GenericRecord record = mapStore.load("0");

        assertThat(record.getString("id")).isEqualTo("0");
        assertThat(record.getString("name")).isEqualTo("name-0");
        mapStore.destroy();
    }

    @Test
    public void givenTable_whenSetColumns_thenGenericRecordHasSetColumns() throws Exception {
        createTable(mapName, "id INT PRIMARY KEY", "name VARCHAR(100)", "age INT", "address VARCHAR(100)");
        executeJdbc("INSERT INTO \"" + mapName + "\" VALUES(0, 'name-0', 42, 'Palo Alto, CA 94306')");

        Properties properties = new Properties();
        properties.setProperty(EXTERNAL_REF_ID_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(COLUMNS_PROPERTY, "id,name,age");
        mapStore = createMapStore(properties, hz);

        GenericRecord record = mapStore.load(0);

        assertThat(record.getInt32("id")).isEqualTo(0);
        assertThat(record.getString("name")).isEqualTo("name-0");
        assertThat(record.getInt32("age")).isEqualTo(42);
        assertThat(record.getFieldKind("address")).isEqualTo(NOT_AVAILABLE);
    }

    @Test
    public void whenSetNonExistingColumn_thenFailToInitialize() throws Exception {
        createTable(mapName, "id INT PRIMARY KEY", "name VARCHAR(100)");
        insertItems(mapName, 1);

        Properties properties = new Properties();
        properties.setProperty(EXTERNAL_REF_ID_PROPERTY, TEST_DATABASE_REF);

        properties.setProperty("columns", "name,age");
        mapStore = createMapStore(properties, hz);

        assertThatThrownBy(() -> mapStore.load(0))
                .isInstanceOf(HazelcastException.class)
                .hasStackTraceContaining("Column 'age' not found");
    }

    @Test
    public void whenSetNonExistingColumnOnSecondMapStore_thenFailToInitialize() throws Exception {
        createTable(mapName, "id INT PRIMARY KEY", "name VARCHAR(100)", "age INT");
        executeJdbc("INSERT INTO " + mapName + " VALUES(0, 'name-0', 42)");
        createMapping(mapName, MAPPING_PREFIX + mapName);
        assertMappingCreated();
        // This simulates a second map store on a different instance. The mapping is created, but must be validated
        // (e.g. the config might differ on members)
        Properties secondProps = new Properties();
        secondProps.setProperty(EXTERNAL_REF_ID_PROPERTY, TEST_DATABASE_REF);
        secondProps.setProperty(COLUMNS_PROPERTY, "id,name,age");
        mapStore = createMapStore(secondProps, hz, false);
        mapStore.init(hz, secondProps, mapName);

        assertThatThrownBy(() -> mapStore.load(0))
                .isInstanceOf(HazelcastException.class)
                .hasStackTraceContaining("Column 'age' not found");
    }

    @Test
    public void givenDefaultTypeName_whenLoad_thenReturnGenericRecordMapNameAsTypeName() throws Exception {
        createTable(mapName);
        insertItems(mapName, 1);

        mapStore = createMapStore();

        CompactGenericRecord record = (CompactGenericRecord) mapStore.load(0);
        assertThat(record.getSchema().getTypeName()).isEqualTo(mapName);
    }

    @Test
    public void givenTypeName_whenLoad_thenReturnGenericRecordWithCorrectTypeName() throws Exception {
        createTable(mapName);
        insertItems(mapName, 1);

        Properties properties = new Properties();
        properties.setProperty(EXTERNAL_REF_ID_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(TYPE_NAME_PROPERTY, "my.Person");
        mapStore = createMapStore(properties, hz);

        CompactGenericRecord record = (CompactGenericRecord) mapStore.load(0);
        assertThat(record.getSchema().getTypeName()).isEqualTo("my.Person");
    }

    @Test
    public void givenRowAndIdColumn_whenLoad_thenReturnGenericRecord() throws Exception {
        createTable(mapName, "\"person-id\" INT PRIMARY KEY", "name VARCHAR(100)");
        insertItems(mapName, 1);

        Properties properties = new Properties();
        properties.setProperty(EXTERNAL_REF_ID_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        mapStore = createMapStore(properties, hz);
        GenericRecord record = mapStore.load(0);

        assertThat(record.getInt32("person-id")).isEqualTo(0);
        assertThat(record.getString("name")).isEqualTo("name-0");
    }

    @Test
    public void givenRowAndColumnsWithoutId_whenLoadAndLoadAll_thenReturnGenericRecordWithoutId() throws Exception {
        createTable(mapName);
        insertItems(mapName, 1);

        Properties properties = new Properties();
        properties.setProperty(EXTERNAL_REF_ID_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(COLUMNS_PROPERTY, "name");
        mapStore = createMapStore(properties, hz);

        GenericRecord record = mapStore.load(0);
        assertThat(record.getFieldKind("id")).isEqualTo(NOT_AVAILABLE);

        Map<Integer, GenericRecord> records = mapStore.loadAll(newArrayList(0));
        assertThat(records.get(0).getFieldKind("id")).isEqualTo(NOT_AVAILABLE);
    }

    @Test
    public void givenRowDoesNotExist_whenLoad_thenReturnNull() throws Exception {
        createTable(mapName);
        mapStore = createMapStore();

        GenericRecord record = mapStore.load(0);
        assertThat(record).isNull();
    }

    @Test
    public void givenRow_whenLoadAll_thenReturnMapWithGenericRecord() throws Exception {
        createTable(mapName);
        mapStore = createMapStore();

        insertItems(mapName, 1);

        Map<Integer, GenericRecord> records = mapStore.loadAll(newArrayList(0));

        assertThat(records).containsKey(0);

        GenericRecord record = records.values().iterator().next();
        records.values().iterator().next();
        assertThat(record.getInt32("id")).isEqualTo(0);
        assertThat(record.getString("name")).isEqualTo("name-0");
    }

    @Test
    public void givenRowAndIdColumn_whenLoadAll_thenReturnGenericRecord() throws Exception {
        createTable(mapName, "\"person-id\" INT PRIMARY KEY", "name VARCHAR(100)");
        insertItems(mapName, 1);

        Properties properties = new Properties();
        properties.setProperty(EXTERNAL_REF_ID_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        mapStore = createMapStore(properties, hz);
        GenericRecord record = mapStore.loadAll(newArrayList(0)).get(0);

        assertThat(record.getInt32("person-id")).isEqualTo(0);
        assertThat(record.getString("name")).isEqualTo("name-0");
    }

    @Test
    public void givenRowDoesNotExist_whenLoadAll_thenReturnEmptyMap() throws Exception {
        createTable(mapName);
        mapStore = createMapStore();

        Map<Integer, GenericRecord> records = mapStore.loadAll(newArrayList(0));
        assertThat(records).isEmpty();
    }

    @Test
    public void givenRow_whenLoadAllKeys_thenReturnKeys() throws Exception {
        createTable(mapName);
        mapStore = createMapStore();

        insertItems(mapName, 1);

        List<Integer> ids = newArrayList(mapStore.loadAllKeys());
        assertThat(ids).contains(0);
    }

    @Test
    public void givenRowAndIdColumn_whenLoadAllKeys_thenReturnKeys() throws Exception {
        createTable(mapName, "\"person-id\" INT PRIMARY KEY", "name VARCHAR(100)");
        insertItems(mapName, 1);

        Properties properties = new Properties();
        properties.setProperty(EXTERNAL_REF_ID_PROPERTY, TEST_DATABASE_REF);

        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        mapStore = createMapStore(properties, hz);

        List<Integer> ids = newArrayList(mapStore.loadAllKeys());
        assertThat(ids).contains(0);
    }

    @Test
    public void givenNoRows_whenLoadAllKeys_thenEmptyIterable() throws Exception {
        createTable(mapName);
        mapStore = createMapStore();

        Iterable<Integer> ids = mapStore.loadAllKeys();
        assertThat(ids).isEmpty();
    }

    @Test
    public void whenStore_thenTableContainsRow() throws Exception {
        createTable(mapName, "\"person-id\" INT PRIMARY KEY", "name VARCHAR(100)");

        Properties properties = new Properties();
        properties.setProperty(EXTERNAL_REF_ID_PROPERTY, TEST_DATABASE_REF);

        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        mapStore = createMapStore(properties, hz);

        GenericRecord person = GenericRecordBuilder.compact("Person")
                                                   .setInt32("person-id", 0)
                                                   .setString("name", "name-0")
                                                   .build();
        mapStore.store(0, person);

        assertJdbcRowsAnyOrder(mapName,
                new Row(0, "name-0")
        );
    }

    @Test
    public void givenIdColumn_whenStore_thenTableContainsRow() throws Exception {
        createTable(mapName);
        mapStore = createMapStore();

        GenericRecord person = GenericRecordBuilder.compact("Person")
                                                   .setInt32("id", 0)
                                                   .setString("name", "name-0")
                                                   .build();
        mapStore.store(0, person);

        assertJdbcRowsAnyOrder(mapName,
                new Row(0, "name-0")
        );
    }

    @Test
    public void givenRow_whenStore_thenRowIsUpdated() throws Exception {
        createTable(mapName);
        insertItems(mapName, 1);

        mapStore = createMapStore();
        GenericRecord person = GenericRecordBuilder.compact("Person")
                                                   .setInt32("id", 0)
                                                   .setString("name", "updated")
                                                   .build();
        mapStore.store(0, person);

        assertJdbcRowsAnyOrder(mapName,
                new Row(0, "updated")
        );
    }

    @Test
    public void givenRowAndIdColumn_whenStore_thenRowIsUpdated() throws Exception {
        createTable(mapName, "\"person-id\" INT PRIMARY KEY", "name VARCHAR(100)");
        insertItems(mapName, 1);

        Properties properties = new Properties();
        properties.setProperty(EXTERNAL_REF_ID_PROPERTY, TEST_DATABASE_REF);

        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        mapStore = createMapStore(properties, hz);

        GenericRecord person = GenericRecordBuilder.compact("Person")
                                                   .setInt32("person-id", 0)
                                                   .setString("name", "updated")
                                                   .build();
        mapStore.store(0, person);

        assertJdbcRowsAnyOrder(mapName,
                new Row(0, "updated")
        );
    }

    @Test
    public void whenStoreAll_thenTableContainsRow() throws Exception {
        createTable(mapName);
        mapStore = createMapStore();

        Map<Integer, GenericRecord> people = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            GenericRecord person = GenericRecordBuilder.compact("Person")
                                                       .setInt32("id", i)
                                                       .setString("name", "name-" + i)
                                                       .build();
            people.put(i, person);
        }
        mapStore.storeAll(people);

        assertJdbcRowsAnyOrder(mapName,
                new Row(0, "name-0"),
                new Row(1, "name-1"),
                new Row(2, "name-2"),
                new Row(3, "name-3"),
                new Row(4, "name-4")
        );
    }

    @Test
    public void whenStoreAllWithNoRecords_thenDoNothing() throws Exception {
        createTable(mapName);
        mapStore = createMapStore();

        mapStore.storeAll(emptyMap());

        assertThat(jdbcRowsTable(mapName)).isEmpty();
    }

    @Test
    public void whenDelete_thenRowRemovedFromTable() throws Exception {
        createTable(mapName);
        insertItems(mapName, 2);

        mapStore = createMapStore();
        mapStore.delete(0);

        assertJdbcRowsAnyOrder(mapName,
                new Row(1, "name-1")
        );
    }

    @Test
    public void givenIdColumn_whenDelete_thenRowRemovedFromTable() throws Exception {
        createTable(mapName, "\"person-id\" INT PRIMARY KEY", "name VARCHAR(100)");
        insertItems(mapName, 2);

        Properties properties = new Properties();
        properties.setProperty(EXTERNAL_REF_ID_PROPERTY, TEST_DATABASE_REF);

        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        mapStore = createMapStore(properties, hz);
        mapStore.delete(0);

        assertJdbcRowsAnyOrder(mapName,
                new Row(1, "name-1")
        );
    }

    @Test
    public void whenDeleteAll_thenRowsRemovedFromTable() throws Exception {
        createTable(mapName);
        insertItems(mapName, 3);

        mapStore = createMapStore();
        mapStore.deleteAll(newArrayList(0, 1));

        assertJdbcRowsAnyOrder(mapName,
                new Row(2, "name-2")
        );
    }

    @Test
    public void givenIdColumn_whenDeleteAll_thenRowRemovedFromTable() throws Exception {
        createTable(mapName, "\"person-id\" INT PRIMARY KEY", "name VARCHAR(100)");
        insertItems(mapName, 2);

        Properties properties = new Properties();
        properties.setProperty(EXTERNAL_REF_ID_PROPERTY, TEST_DATABASE_REF);

        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        mapStore = createMapStore(properties, hz);
        mapStore.deleteAll(newArrayList(0));

        assertJdbcRowsAnyOrder(mapName,
                new Row(1, "name-1")
        );
    }

    @Test
    public void whenDeleteAllWithNoIds_thenDoNothing() throws Exception {
        createTable(mapName);
        insertItems(mapName, 1);

        mapStore = createMapStore();
        mapStore.deleteAll(newArrayList());

        assertJdbcRowsAnyOrder(mapName,
                new Row(0, "name-0")
        );
    }

    @Test
    public void givenMapStoreConfigWithOffloadDisabled_thenFail() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setClassName(GenericMapStore.class.getName())
                .setOffload(false);

        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        instance().getConfig().addMapConfig(mapConfig);

        mapStore = new GenericMapStore<>();
        Properties properties = new Properties();
        properties.setProperty(EXTERNAL_REF_ID_PROPERTY, TEST_DATABASE_REF);

        assertThatThrownBy(() -> mapStore.init(hz, properties, mapName))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Config for GenericMapStore must have `offload` property set to true");
    }

    @Test
    public void givenTableNameProperty_whenCreateMapStore_thenUseTableName() throws Exception {
        String tableName = randomTableName();

        createTable(tableName);
        insertItems(tableName, 1);

        Properties properties = new Properties();
        properties.setProperty(EXTERNAL_REF_ID_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(TABLE_NAME_PROPERTY, tableName);
        mapStore = createMapStore(properties, hz);

        GenericRecord record = mapStore.load(0);
        assertThat(record).isNotNull();
    }

    @Test
    @Ignore("https://github.com/hazelcast/hazelcast/issues/22527")
    public void givenColumnPropSubset_whenStore_thenTableContainsRow() throws SQLException {
        createTable(mapName, "id INT PRIMARY KEY", "name VARCHAR(100)", "other VARCHAR(100) DEFAULT 'def'");
        try (Connection conn = DriverManager.getConnection(dbConnectionUrl);
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute("INSERT INTO " + mapName + " (id, name) VALUES(0, 'name-0')");
        }

        Properties properties = new Properties();
        properties.setProperty(EXTERNAL_REF_ID_PROPERTY, TEST_DATABASE_REF);

        properties.setProperty("columns", "id,name");
        mapStore = createMapStore(properties, hz);

        GenericRecord person = GenericRecordBuilder.compact(mapName)
                .setInt32("id", 1)
                .setString("name", "name-1")
                .build();
        mapStore.store(1, person);


        assertJdbcRowsAnyOrder(mapName,
                new Row(0, "name-0"),
                new Row(1, "name-1")
        );
    }

    private <K> GenericMapStore<K> createMapStore() {
        return createMapStore(hz);
    }

    private <K> GenericMapStore<K> createMapStore(HazelcastInstance instance) {
        Properties properties = new Properties();
        properties.setProperty(EXTERNAL_REF_ID_PROPERTY, TEST_DATABASE_REF);
        return createMapStore(properties, instance);
    }

    private <K> GenericMapStore<K> createMapStore(Properties properties, HazelcastInstance instance) {
        return createMapStore(properties, instance, true);
    }

    private <K> GenericMapStore<K> createMapStore(Properties properties, HazelcastInstance instance, boolean init) {
        MapConfig mapConfig = createMapConfigWithMapStore(mapName);
        instance.getConfig().addMapConfig(mapConfig);

        GenericMapStore<K> mapStore = new GenericMapStore<>();
        if (init) {
            mapStore.init(instance, properties, mapName);
            mapStore.awaitInitFinished();
        }
        return mapStore;
    }

    private static MapConfig createMapConfigWithMapStore(String mapName) {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setClassName(GenericMapStore.class.getName());
        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        return mapConfig;
    }

    private void assertMappingCreated() {
        assertTrueEventually(() -> {
            assertRowsAnyOrder(hz, "SHOW MAPPINGS", newArrayList(new Row(MAPPING_PREFIX + mapName)));
        }, 60);
    }

    private void assertMappingDestroyed() {
        assertTrueEventually(() -> assertRowsAnyOrder(hz, "SHOW MAPPINGS", newArrayList()), 60);
    }
}
