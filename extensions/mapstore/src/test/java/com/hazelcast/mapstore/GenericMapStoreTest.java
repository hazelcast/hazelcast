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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.test.SerialTest;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.mapstore.GenericMapStore.DATA_CONNECTION_REF_PROPERTY;
import static com.hazelcast.mapstore.GenericMapStore.EXTERNAL_NAME_PROPERTY;
import static com.hazelcast.mapstore.GenericMapStore.ID_COLUMN_PROPERTY;
import static com.hazelcast.mapstore.GenericMapStore.MAPPING_PREFIX;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.newArrayList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This test runs the MapStore methods directly, but it runs within real Hazelcast instance
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({SerialTest.class})
public class GenericMapStoreTest extends GenericMapLoaderTest {

    private GenericMapStore<Integer> mapStore;

    @After
    public void after() {
        if (mapStore != null && mapStore.initHasFinished()) {
            mapStore.destroy();
            mapStore = null;
        }
    }

    @Test
    public void validIntegrityConstraintViolation() {
        SQLException sqlException = new SQLException("reason", "2300");
        JetException jetException = new JetException(sqlException);

        boolean integrityConstraintViolation = GenericMapStore.isIntegrityConstraintViolation(jetException);
        assertTrue(integrityConstraintViolation);
    }

    @Test
    public void invalidIntegrityConstraintViolation() {
        SQLException sqlException = new SQLException("reason", "2000");
        JetException jetException = new JetException(sqlException);

        boolean integrityConstraintViolation = GenericMapStore.isIntegrityConstraintViolation(jetException);
        assertFalse(integrityConstraintViolation);
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
    public void whenStore_thenTableContainsRow() throws Exception {
        createTable(mapName, quote("person-id") + " INT PRIMARY KEY", "name VARCHAR(100)");

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);

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
        createTable(mapName, quote("person-id") + " INT PRIMARY KEY", "name VARCHAR(100)");
        insertItems(mapName, 1);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);

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
        createTable(mapName, quote("person-id") + " INT PRIMARY KEY", "name VARCHAR(100)");
        insertItems(mapName, 2);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);

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
        createTable(mapName, quote("person-id") + " INT PRIMARY KEY", "name VARCHAR(100)");
        insertItems(mapName, 2);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);

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
    public void givenTableNameProperty_whenCreateMapStore_thenUseTableName() throws Exception {
        String tableName = randomTableName();

        createTable(tableName);
        insertItems(tableName, 1);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(EXTERNAL_NAME_PROPERTY, tableName);
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
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);

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
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        return createMapStore(properties, instance);
    }

    private <K> GenericMapStore<K> createMapStore(Properties properties, HazelcastInstance instance) {
        return createUnitUnderTest(properties, instance, true);
    }

    @Override
    protected <K> GenericMapStore<K> createUnitUnderTest(Properties properties, HazelcastInstance instance, boolean init) {
        MapConfig mapConfig = createMapConfigWithMapStore(mapName, properties);
        instance.getConfig().addMapConfig(mapConfig);

        GenericMapStore<K> mapStore = new GenericMapStore<>();
        if (init) {
            mapStore.init(instance, properties, mapName);
            mapStore.awaitInitFinished();
        }
        return mapStore;
    }

    private MapConfig createMapConfigWithMapStore(String mapName, Properties properties) {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setClassName(GenericMapStore.class.getName());
        mapStoreConfig.setProperties(properties);
        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        return mapConfig;
    }
}
