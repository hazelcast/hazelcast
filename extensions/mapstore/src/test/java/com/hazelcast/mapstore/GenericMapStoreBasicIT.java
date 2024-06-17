/*
 * Copyright 2024 Hazelcast Inc.
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
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.jdbc.TestDatabaseRecordProvider.ObjectSpec;
import com.hazelcast.test.jdbc.JdbcObjectProvider;
import org.junit.After;
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

import static com.hazelcast.mapstore.GenericMapLoader.SINGLE_COLUMN_AS_VALUE;
import static com.hazelcast.mapstore.GenericMapStore.DATA_CONNECTION_REF_PROPERTY;
import static com.hazelcast.mapstore.GenericMapStore.EXTERNAL_NAME_PROPERTY;
import static com.hazelcast.mapstore.GenericMapStore.ID_COLUMN_PROPERTY;
import static com.hazelcast.mapstore.GenericMapStore.MAPPING_PREFIX;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.util.Lists.newArrayList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This test runs the MapStore methods directly, but it runs within real Hazelcast instance
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, SerialTest.class})
public class GenericMapStoreBasicIT extends GenericMapLoaderTest {

    private GenericMapStore<Integer, GenericRecord> mapStore;
    private GenericMapStore<Integer, String> mapStoreSingleColAsValue;

    @After
    public void after() {
        if (mapStore != null && mapStore.initHasFinished()) {
            mapStore.destroy();
            mapStore = null;
        }
        if (mapStoreSingleColAsValue != null && mapStoreSingleColAsValue.initHasFinished()) {
            mapStoreSingleColAsValue.destroy();
            mapStoreSingleColAsValue = null;
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
    public void whenMapStoreInit_thenCreateMappingForMapStoreConfig() {
        objectProvider.createObject(mapName, false);

        mapStore = createMapStore();
        assertMappingCreated();
    }

    @Test
    public void whenMapStoreInit_thenCreateMappingForMapStoreSingleColAsValueConfig() {
        objectProvider.createObject(mapName);

        mapStore = createMapStoreSingleColumnAsValue();
        assertMappingCreated();
    }

    @Test
    public void whenMapStoreInitCalledOnNonMaster_thenInitAndLoadValue() {
        ObjectSpec spec = objectProvider.createObject(mapName, false);
        objectProvider.insertItems(spec, 1);

        mapStore = createMapStore(instances()[1]);
        GenericRecord record = mapStore.load(0);
        assertThat(record).isNotNull();
    }

    @Test
    public void whenMapStoreInitCalledOnNonMaster_thenInitAndLoadSingleColAsValue() {
        ObjectSpec spec = objectProvider.createObject(mapName);
        objectProvider.insertItems(spec, 1);

        mapStoreSingleColAsValue = createMapStoreSingleColumnAsValue(instances()[1]);
        String name = mapStoreSingleColAsValue.load(0);
        assertThat(name).isNotNull();
    }

    @Test
    public void givenValidMappingExists_whenMapStoreInit_thenInitAndLoadRecord() {
        ObjectSpec spec = objectProvider.createObject(mapName, false);
        objectProvider.insertItems(spec, 1);
        createMapping(mapName, MAPPING_PREFIX + mapName);

        mapStore = createMapStore();
        GenericRecord loaded = mapStore.load(0);
        assertThat(loaded).isNotNull();
    }

    @Test
    public void givenValidMappingExists_whenMapStoreInit_thenInitAndLoadSingleColAsValue() {
        ObjectSpec spec = objectProvider.createObject(mapName);
        objectProvider.insertItems(spec, 1);
        createMapping(mapName, MAPPING_PREFIX + mapName);

        mapStoreSingleColAsValue = createMapStoreSingleColumnAsValue();
        String loaded = mapStoreSingleColAsValue.load(0);
        assertThat(loaded).isNotNull();
    }

    @Test
    public void whenMapStoreDestroyOnMaster_thenDropMapping() {
        objectProvider.createObject(mapName, false);

        mapStore = createMapStore();
        assertMappingCreated();

        mapStore.destroy();
        assertMappingDestroyed();
    }

    @Test
    public void whenMapStoreDestroyOnNonMaster_thenDropMapping() {
        objectProvider.createObject(mapName, false);

        mapStore = createMapStore();
        assertMappingCreated();

        GenericMapStore<Object, GenericRecord> mapStoreNotMaster = createMapStore(instances()[1]);
        mapStoreNotMaster.destroy();
        assertMappingDestroyed();
    }

    @Test
    public void whenMapStoreInitOnNonMaster_thenLoadWaitsForSuccessfulInit() {
        ObjectSpec spec = objectProvider.createObject(mapName, false);
        objectProvider.insertItems(spec, 1);

        GenericMapStore<Object, GenericRecord> mapStoreNonMaster = createMapStore(instances()[1]);
        mapStore = createMapStore();

        GenericRecord record = mapStoreNonMaster.load(0);
        assertThat(record).isNotNull();
    }

    @Test
    public void whenStore_thenTableContainsRow() {
        objectProvider.createObject(mapName, true);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);

        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        mapStore = createMapStore(properties, hz);

        GenericRecord person = GenericRecordBuilder.compact("Person")
                                                   .setInt32("person-id", 0)
                                                   .setString("name", "name-0")
                                                   .build();
        mapStore.store(0, person);

        objectProvider.assertRow(mapName,
                newArrayList(Integer.class, String.class),
                asList(0, "name-0")
        );
    }

    @Test
    public void givenIdColumn_whenStore_thenTableContainsRow() {
        objectProvider.createObject(mapName, false);
        mapStore = createMapStore();

        GenericRecord person = GenericRecordBuilder.compact("Person")
                                                   .setInt32("id", 0)
                                                   .setString("name", "name-0")
                                                   .build();
        mapStore.store(0, person);

        objectProvider.assertRow(mapName,
                newArrayList(Integer.class, String.class),
                asList(0, "name-0")
        );
    }

    @Test
    public void givenRow_whenStore_thenRowIsUpdated() {
        ObjectSpec spec = objectProvider.createObject(mapName, false);
        objectProvider.insertItems(spec, 1);

        mapStore = createMapStore();
        GenericRecord person = GenericRecordBuilder.compact("Person")
                                                   .setInt32("id", 0)
                                                   .setString("name", "updated")
                                                   .build();
        mapStore.store(0, person);

        objectProvider.assertRow(mapName,
                newArrayList(Integer.class, String.class),
                asList(0, "updated")
        );
    }

    @Test
    public void givenRowAndIdColumn_whenStore_thenRowIsUpdated() {
        ObjectSpec spec = objectProvider.createObject(mapName, true);
        objectProvider.insertItems(spec, 1);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);

        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        mapStore = createMapStore(properties, hz);

        GenericRecord person = GenericRecordBuilder.compact("Person")
                                                   .setInt32("person-id", 0)
                                                   .setString("name", "updated")
                                                   .build();
        mapStore.store(0, person);

        objectProvider.assertRow(mapName,
                newArrayList(Integer.class, String.class),
                asList(0, "updated")
        );
    }

    @Test
    public void whenStoreAll_thenTableContainsRow() {
        objectProvider.createObject(mapName, false);
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

        objectProvider.assertRows(mapName,
                newArrayList(Integer.class, String.class),
                List.of(
                asList(0, "name-0"),
                asList(1, "name-1"),
                asList(2, "name-2"),
                asList(3, "name-3"),
                asList(4, "name-4")
        ));
    }

    @Test
    public void whenStoreAllWithNoRecords_thenDoNothing() {
        objectProvider.createObject(mapName, false);
        mapStore = createMapStore();

        mapStore.storeAll(emptyMap());

        objectProvider.assertRows(mapName, emptyList());
    }

    @Test
    public void whenDelete_thenRowRemovedFromTable() {
        ObjectSpec spec = objectProvider.createObject(mapName, false);
        objectProvider.insertItems(spec, 2);

        mapStore = createMapStore();
        mapStore.delete(0);

        objectProvider.assertRow(mapName,
                newArrayList(Integer.class, String.class),
                asList(1, "name-1")
        );
    }

    @Test
    public void givenIdColumn_whenDelete_thenRowRemovedFromTable() {
        ObjectSpec spec = objectProvider.createObject(mapName, true);
        objectProvider.insertItems(spec, 2);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);

        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        mapStore = createMapStore(properties, hz);
        mapStore.delete(0);

        objectProvider.assertRow(mapName,
                newArrayList(Integer.class, String.class),
                asList(1, "name-1")
        );
    }

    @Test
    public void whenDeleteAll_thenRowsRemovedFromTable() {
        ObjectSpec spec = objectProvider.createObject(mapName, false);
        objectProvider.insertItems(spec, 3);

        mapStore = createMapStore();
        mapStore.deleteAll(newArrayList(0, 1));

        objectProvider.assertRow(mapName,
                newArrayList(Integer.class, String.class),
                asList(2, "name-2")
        );
    }

    @Test
    public void givenIdColumn_whenDeleteAll_thenRowRemovedFromTable() {
        ObjectSpec spec = objectProvider.createObject(mapName, true);
        objectProvider.insertItems(spec, 2);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);

        properties.setProperty(ID_COLUMN_PROPERTY, "person-id");
        mapStore = createMapStore(properties, hz);
        mapStore.deleteAll(newArrayList(0));

        objectProvider.assertRow(mapName,
                newArrayList(Integer.class, String.class),
                asList(1, "name-1")
        );
    }

    @Test
    public void whenDeleteAllWithNoIds_thenDoNothing() {
        ObjectSpec spec = objectProvider.createObject(mapName, false);
        objectProvider.insertItems(spec, 1);

        mapStore = createMapStore();
        mapStore.deleteAll(newArrayList());

        objectProvider.assertRow(mapName,
                newArrayList(Integer.class, String.class),
                asList(0, "name-0")
        );
    }

    @Test
    public void givenTableNameProperty_whenCreateMapStore_thenUseTableName() {
        String tableName = randomName();

        ObjectSpec spec = objectProvider.createObject(tableName, false);
        objectProvider.insertItems(spec, 1);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(EXTERNAL_NAME_PROPERTY, tableName);
        mapStore = createMapStore(properties, hz);

        GenericRecord record = mapStore.load(0);
        assertThat(record).isNotNull();
    }

    @Test
    public void givenTableNameProperty_whenCreateMapStoreSingleColAsValue_thenUseTableName() {
        String tableName = randomName();

        ObjectSpec spec = objectProvider.createObject(tableName);
        objectProvider.insertItems(spec, 1);

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(EXTERNAL_NAME_PROPERTY, tableName);
        properties.setProperty(SINGLE_COLUMN_AS_VALUE, "true");
        mapStoreSingleColAsValue = createMapStore(properties, hz);

        String name = mapStoreSingleColAsValue.load(0);
        assertThat(name).isNotNull();
    }

    @Test
    public void givenColumnPropSubset_whenStore_thenTableContainsRow() throws SQLException {
        assumeThat(objectProvider).isInstanceOf(JdbcObjectProvider.class);
        var jdbcDbProvider = (JdbcObjectProvider) objectProvider;
        jdbcDbProvider.createTable(mapName, "id INT PRIMARY KEY", "name VARCHAR(100)", "other VARCHAR(100) DEFAULT 'def'");
        try (Connection conn = DriverManager.getConnection(dbConnectionUrl);
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute(String.format("INSERT INTO %s (%s, %s) VALUES(0, 'name-0')",
                    databaseProvider.quote(mapName),
                    databaseProvider.quote("id"),
                    databaseProvider.quote("name")));
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


        objectProvider.assertRows(mapName,
                newArrayList(Integer.class, String.class, String.class),
                List.of(
                asList(0, "name-0", "def"),
                asList(1, "name-1", "def")
        ));
    }

    @Test
    public void givenColumnPropSubset_whenStoreAsSingleColumn_thenTableContainsRow() throws SQLException {
        assumeThat(objectProvider).isInstanceOf(JdbcObjectProvider.class);
        var jdbcDbProvider = (JdbcObjectProvider) objectProvider;
        jdbcDbProvider.createTable(mapName, "id INT PRIMARY KEY", "name VARCHAR(100)", "other VARCHAR(100) DEFAULT 'def'");
        try (Connection conn = DriverManager.getConnection(dbConnectionUrl);
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute(String.format("INSERT INTO %s (%s, %s) VALUES(0, 'name-0')",
                    databaseProvider.quote(mapName),
                    databaseProvider.quote("id"),
                    databaseProvider.quote("name")));
        }

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(SINGLE_COLUMN_AS_VALUE, "true");

        properties.setProperty("columns", "name");
        mapStoreSingleColAsValue = createMapStore(properties, hz);

        String name = "name-1";
        mapStoreSingleColAsValue.store(1, name);


        objectProvider.assertRows(mapName,
                newArrayList(Integer.class, String.class, String.class),
                List.of(
                asList(0, "name-0", "def"),
                asList(1, "name-1", "def")
        ));
    }

    @Test
    public void givenColumnPropSubsetWithoutId_whenStore_thenTableContainsRow() throws SQLException {
        assumeThat(objectProvider).isInstanceOf(JdbcObjectProvider.class);
        var jdbcDbProvider = (JdbcObjectProvider) objectProvider;
        jdbcDbProvider.createTable(mapName, "id INT PRIMARY KEY", "name VARCHAR(100)", "other VARCHAR(100) DEFAULT 'def'");
        try (Connection conn = DriverManager.getConnection(dbConnectionUrl);
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute(String.format("INSERT INTO %s (%s, %s) VALUES(0, 'name-0')",
                    databaseProvider.quote(mapName),
                    databaseProvider.quote("id"),
                    databaseProvider.quote("name")));
        }

        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);

        properties.setProperty("columns", "name");
        mapStore = createMapStore(properties, hz);

        GenericRecord person = GenericRecordBuilder.compact(mapName)
                .setString("name", "name-1")
                .build();
        mapStore.store(1, person);

        objectProvider.assertRows(mapName,
                newArrayList(Integer.class, String.class, String.class),
                List.of(
                asList(0, "name-0", "def"),
                asList(1, "name-1", "def")
        ));
    }

    private <K, V> GenericMapStore<K, V> createMapStore() {
        return createMapStore(hz);
    }

    private <K, V> GenericMapStore<K, V> createMapStoreSingleColumnAsValue() {
        return createMapStoreSingleColumnAsValue(hz);
    }

    private <K, V> GenericMapStore<K, V> createMapStore(HazelcastInstance instance) {
        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        return createMapStore(properties, instance);
    }


    private <K, V> GenericMapStore<K, V> createMapStoreSingleColumnAsValue(HazelcastInstance instance) {
        Properties properties = new Properties();
        properties.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        properties.setProperty(SINGLE_COLUMN_AS_VALUE, "true");
        return createMapStore(properties, instance);
    }

    private <K, V> GenericMapStore<K, V> createMapStore(Properties properties, HazelcastInstance instance) {
        return createUnitUnderTest(properties, instance, true);
    }

    @Override
    protected <K, V> GenericMapStore<K, V> createUnitUnderTest(Properties properties,
                                                               HazelcastInstance instance,
                                                               boolean init) {
        MapConfig mapConfig = createMapConfigWithMapStore(mapName, properties);
        instance.getConfig().addMapConfig(mapConfig);

        GenericMapStore<K, V> mapStore = new GenericMapStore<>();
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
