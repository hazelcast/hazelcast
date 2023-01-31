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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ExternalDataStoreConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.datastore.JdbcDataStoreFactory;
import com.hazelcast.internal.util.FilteringClassLoader;
import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlTestSupport;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.example.Person;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.mapstore.GenericMapStore.EXTERNAL_REF_ID_PROPERTY;
import static com.hazelcast.mapstore.GenericMapStore.TYPE_NAME_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Lists.newArrayList;

public class GenericMapStoreIntegrationTest extends JdbcSqlTestSupport {

    private String tableName;

    @BeforeClass
    public static void beforeClass() {
        databaseProvider = new H2DatabaseProvider();
        dbConnectionUrl = databaseProvider.createDatabase(JdbcSqlTestSupport.class.getName());

        Config config = smallInstanceConfig();
        // Need to set filtering class loader so the members don't deserialize into class but into GenericRecord
        config.setClassLoader(new FilteringClassLoader(newArrayList("org.example"), null));

        config.addExternalDataStoreConfig(
                new ExternalDataStoreConfig(TEST_DATABASE_REF)
                        .setClassName(JdbcDataStoreFactory.class.getName())
                        .setProperty("jdbcUrl", dbConnectionUrl)
        );


        ClientConfig clientConfig = new ClientConfig();

        initializeWithClient(2, config, clientConfig);
        sqlService = instance().getSql();
    }

    @Before
    public void setUp() throws Exception {
        tableName = randomTableName();
        createTable(tableName);
        insertItems(tableName, 1);

        MapConfig mapConfig = new MapConfig(tableName);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setClassName(GenericMapStore.class.getName());
        mapStoreConfig.setProperty(EXTERNAL_REF_ID_PROPERTY, TEST_DATABASE_REF);
        mapStoreConfig.setProperty(TYPE_NAME_PROPERTY, "org.example.Person");
        mapConfig.setMapStoreConfig(mapStoreConfig);
        instance().getConfig().addMapConfig(mapConfig);
    }

    @Test
    public void testGet() {
        HazelcastInstance client = client();
        IMap<Integer, Person> map = client.getMap(tableName);

        Person p = map.get(0);
        assertThat(p.getId()).isEqualTo(0);
        assertThat(p.getName()).isEqualTo("name-0");
    }

    @Test
    public void testPut() {
        HazelcastInstance client = client();
        IMap<Integer, Person> map = client.getMap(tableName);

        map.put(42, new Person(42, "name-42"));

        assertJdbcRowsAnyOrder(tableName,
                new Row(0, "name-0"),
                new Row(42, "name-42")
        );
    }

    @Test
    public void testPutWhenExists() {
        HazelcastInstance client = client();
        IMap<Integer, Person> map = client.getMap(tableName);

        assertJdbcRowsAnyOrder(tableName,
                new Row(0, "name-0")
        );

        map.put(0, new Person(0, "updated"));

        assertJdbcRowsAnyOrder(tableName,
                new Row(0, "updated")
        );
    }

    @Test
    public void testRemove() {
        HazelcastInstance client = client();
        IMap<Integer, Person> map = client.getMap(tableName);

        assertThat(jdbcRowsTable(tableName)).hasSize(1);

        map.remove(0);

        assertThat(jdbcRowsTable(tableName)).isEmpty();
    }

    @Test
    public void testDynamicExternalDataStoreConfig() throws Exception {
        String randomTableName = randomTableName();

        createTable(randomTableName);
        assertThat(jdbcRowsTable(randomTableName)).isEmpty();

        HazelcastInstance client = client();

        client.getConfig().addExternalDataStoreConfig(
                new ExternalDataStoreConfig("dynamically-added-datastore")
                        .setClassName(JdbcDataStoreFactory.class.getName())
                        .setProperty("jdbcUrl", dbConnectionUrl)
        );

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setClassName(GenericMapStore.class.getName())
                .setProperty(EXTERNAL_REF_ID_PROPERTY, "dynamically-added-datastore")
                .setProperty("table-name", randomTableName);
        MapConfig mapConfig = new MapConfig(randomTableName).setMapStoreConfig(mapStoreConfig);
        client.getConfig().addMapConfig(mapConfig);

        IMap<Integer, Person> someTestMap = client.getMap(randomTableName);
        someTestMap.put(42, new Person(42, "some-name-42"));

        assertJdbcRowsAnyOrder(randomTableName,
                new Row(42, "some-name-42")
        );

    }

    /**
     * https://github.com/hazelcast/hazelcast/issues/22570
     */
    @Test
    public void testExecuteOnEntries() {
        HazelcastInstance client = client();
        IMap<Object, Object> map = client.getMap(tableName);
        map.loadAll(false);

        map.executeOnEntries((EntryProcessor<Object, Object, Object>) entry -> {
            GenericRecord rec = (GenericRecord) entry.getValue();
            GenericRecord modifiedRecord = rec.newBuilderWithClone()
                    .setString("name", "new-name-" + rec.getInt32("id"))
                    .build();
            entry.setValue(modifiedRecord);
            return null;
        });

        assertJdbcRowsAnyOrder(tableName,
                new Row(0, "new-name-0")
        );
    }

    @Test
    public void testMapClear() {
        HazelcastInstance client = client();
        IMap<Integer, Person> map = client.getMap(tableName);
        map.loadAll(false);

        map.clear();

        assertThat(jdbcRowsTable(tableName)).isEmpty();
    }

    @Test
    public void testPutWithColumnMismatch() {
        HazelcastInstance client = client();
        IMap<Integer, GenericRecord> map = client.getMap(tableName);

        assertThatThrownBy(() -> {
            map.put(42,
                    GenericRecordBuilder.compact("org.example.Person")
                            .setString("id", "42")
                            .setString("name", "name-42")
                            .build()
            );
        })
                .isInstanceOf(HazelcastSerializationException.class)
                .hasMessageContaining("Invalid field kind: 'id for Schema" +
                        " { className = org.example.Person, numberOfComplexFields = 2," +
                        " primitivesLength = 0, map = {name=FieldDescriptor{" +
                        "name='name', kind=STRING, index=1, offset=-1, bitOffset=-1}," +
                        " id=FieldDescriptor{name='id', kind=STRING, index=0, " +
                        "offset=-1, bitOffset=-1}}}, valid field kinds : " +
                        "[INT32, NULLABLE_INT32], found : STRING");
    }

    @Test
    public void testRemoveWhenNotExists() {
        HazelcastInstance client = client();
        IMap<Integer, Person> map = client.getMap(tableName);

        assertThat(jdbcRowsTable(tableName)).hasSize(1);

        assertThat(map.remove(1)).isNull();

        assertThat(jdbcRowsTable(tableName)).hasSize(1);
    }

    @Test
    public void testRemoveWhenExistsInTableOnly() {
        HazelcastInstance client = client();
        IMap<Integer, Person> map = client.getMap(tableName);

        map.put(42, new Person(42, "name-42"));
        map.evictAll();

        assertThat(map.size()).isEqualTo(0);
        assertThat(jdbcRowsTable(tableName)).hasSize(2);

        Person p = map.remove(0);
        assertThat(p.getId()).isEqualTo(0);
        assertThat(p.getName()).isEqualTo("name-0");

        assertThat(jdbcRowsTable(tableName)).hasSize(1);
    }

    @Test
    public void testPutAll() {
        HazelcastInstance client = client();
        IMap<Integer, Person> map = client.getMap(tableName);

        Map<Integer, Person> putMap = new HashMap<>();
        putMap.put(42, new Person(42, "name-42"));
        putMap.put(43, new Person(43, "name-43"));
        putMap.put(44, new Person(44, "name-44"));
        map.putAll(putMap);

        assertJdbcRowsAnyOrder(tableName,
                new Row(0, "name-0"),
                new Row(42, "name-42"),
                new Row(43, "name-43"),
                new Row(44, "name-44")
        );
    }

    @Test
    public void testPutAllWhenExists() {
        HazelcastInstance client = client();
        IMap<Integer, Person> map = client.getMap(tableName);

        Map<Integer, Person> putMap = new LinkedHashMap<>();
        putMap.put(42, new Person(42, "name-42"));
        putMap.put(0, new Person(0, "updated"));
        putMap.put(44, new Person(44, "name-44"));
        map.putAll(putMap);

        assertJdbcRowsAnyOrder(tableName,
                new Row(0, "updated"),
                new Row(42, "name-42"),
                new Row(44, "name-44")
        );
    }

    @Test
    public void testExceptionIsConstructable() {
        HazelcastInstance client = client();
        IMap<Integer, Person> map = client.getMap(tableName);
        map.loadAll(false);

        execute("DROP MAPPING \"__map-store." + tableName + "\"");

        String message = "did you forget to CREATE MAPPING?";
        Person person = new Person(42, "name-42");
        assertThatThrownBy(() -> map.put(42, person))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining(message)
                .hasCauseInstanceOf(QueryException.class)
                .hasStackTraceContaining(message);

        assertThat(map.size()).isEqualTo(1);
    }

    @Test
    public void testDestroy() {
        HazelcastInstance client = client();
        IMap<Integer, Person> map = client.getMap(tableName);
        map.loadAll(false);

        map.destroy();

        Row row = new Row("__map-store." + tableName);
        List<Row> rows = Arrays.asList(row);
        assertTrueEventually(() -> {
            assertDoesNotContainRow(client, "SHOW MAPPINGS", rows);
        }, 5);
    }
}
