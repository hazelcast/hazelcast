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
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.FilteringClassLoader;
import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlTestSupport;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.test.ExceptionRecorder;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import com.hazelcast.test.jdbc.TestDatabaseProvider;
import org.example.Person;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;

import static com.hazelcast.mapstore.GenericMapLoader.EXTERNAL_NAME_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.LOAD_ALL_KEYS_PROPERTY;
import static com.hazelcast.mapstore.GenericMapStore.DATA_CONNECTION_REF_PROPERTY;
import static com.hazelcast.mapstore.GenericMapStore.TYPE_NAME_PROPERTY;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Lists.newArrayList;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class GenericMapStoreIT extends JdbcSqlTestSupport {

    private static Config memberConfig;

    @Rule
    public TestName testName = new TestName();

    private String prefix = "generic_";

    private String tableName;

    protected void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @BeforeClass
    public static void beforeClass() {
        assumeDockerEnabled();
        initializeBeforeClass(new H2DatabaseProvider());
    }

    protected static void initializeBeforeClass(TestDatabaseProvider testDatabaseProvider) {
        databaseProvider = testDatabaseProvider;
        dbConnectionUrl = databaseProvider.createDatabase(JdbcSqlTestSupport.class.getName());

        // Do not use small config to run with more threads and partitions
        memberConfig = new Config()
                // Need to set filtering class loader so the members don't deserialize into class but into GenericRecord
                .setClassLoader(new FilteringClassLoader(newArrayList("org.example"), null))
                .addDataConnectionConfig(
                        new DataConnectionConfig(TEST_DATABASE_REF)
                                .setType("jdbc")
                                .setProperty("jdbcUrl", dbConnectionUrl)
                );
        memberConfig.getJetConfig().setEnabled(true);

        ClientConfig clientConfig = new ClientConfig();

        initializeWithClient(2, memberConfig, clientConfig);
        sqlService = instance().getSql();
    }

    @Before
    public void setUp() throws Exception {
        tableName = prefix + testName.getMethodName().toLowerCase(Locale.ROOT);
        createTable(tableName);
        insertItems(tableName, 1);

        MapConfig mapConfig = new MapConfig(tableName);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setClassName(GenericMapStore.class.getName());
        mapStoreConfig.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        mapStoreConfig.setProperty(TYPE_NAME_PROPERTY, "org.example.Person");
        mapConfig.setMapStoreConfig(mapStoreConfig);
        instance().getConfig().addMapConfig(mapConfig);
    }

    @Test
    public void testLoadAllKeys() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setClassName(GenericMapStore.class.getName());
        mapStoreConfig.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        mapStoreConfig.setProperty(EXTERNAL_NAME_PROPERTY, tableName);
        mapStoreConfig.setProperty(TYPE_NAME_PROPERTY, "org.example.Person");
        mapStoreConfig.setProperty(LOAD_ALL_KEYS_PROPERTY, "false");

        String mapName = tableName + "_disabled";
        MapConfig mapConfig = new MapConfig(mapName);

        mapConfig.setMapStoreConfig(mapStoreConfig);
        instance().getConfig().addMapConfig(mapConfig);

        HazelcastInstance client = client();
        IMap<Integer, Person> map = client.getMap(mapName);
        assertThat(map.size()).isZero();

        // LOAD_ALL_KEYS_PROPERTY is disabled, but we still can load
        Person p = map.get(0);
        assertThat(p.getId()).isZero();
        assertThat(p.getName()).isEqualTo("name-0");
    }

    @Test
    public void testGet() {
        HazelcastInstance client = client();
        IMap<Integer, Person> map = client.getMap(tableName);

        Person p = map.get(0);
        assertThat(p.getId()).isZero();
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
    public void testDynamicDataConnectionConfig() throws Exception {
        String randomTableName = randomTableName();

        createTable(randomTableName);
        assertThat(jdbcRowsTable(randomTableName)).isEmpty();

        HazelcastInstance client = client();

        client.getConfig().addDataConnectionConfig(
                new DataConnectionConfig("dynamically-added-data-connection")
                        .setType("jdbc")
                        .setProperty("jdbcUrl", dbConnectionUrl)
        );

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setClassName(GenericMapStore.class.getName())
                .setProperty(DATA_CONNECTION_REF_PROPERTY, "dynamically-added-data-connection")
                .setProperty(EXTERNAL_NAME_PROPERTY, randomTableName);
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
    public void testPutWithGenericRecordIdColumnIgnored() {
        HazelcastInstance client = client();
        IMap<Integer, GenericRecord> map = client.getMap(tableName);

        map.put(400,
                GenericRecordBuilder.compact("org.example.Person")
                        .setString("id", "42")
                        .setString("name", "name-400")
                        .build()
        );

        assertJdbcRowsAnyOrder(tableName,
                new Row(0, "name-0"),
                new Row(400, "name-400")
        );
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

        assertThat(map.size()).isZero();
        assertThat(jdbcRowsTable(tableName)).hasSize(2);

        Person p = map.remove(0);
        assertThat(p.getId()).isZero();
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
    public void testExceptionIsConstructable() throws SQLException {
        HazelcastInstance client = client();
        // Add some more items for all members
        insertItems(tableName, 2, 5);

        IMap<Integer, Person> map = client.getMap(tableName);
        // Method call to create the lazy mapping in GenericMapStore
        map.loadAll(false);

        // This should ensure that all members have created the mapping
        assertTrueEventually(() -> assertThat(map.size()).isEqualTo(6));

        String mappingName = "__map-store." + tableName;
        execute("DROP MAPPING \"" + mappingName + "\"");

        // DROP MAPPING is executed asynchronously. Ensure that it has finished
        Row row = new Row(mappingName);
        List<Row> rows = Collections.singletonList(row);
        assertTrueEventually(() -> assertDoesNotContainRow(client, "SHOW MAPPINGS", rows), 30);

        String message = "did you forget to CREATE MAPPING?";
        Person person = new Person(42, "name-42");
        assertThatThrownBy(() -> map.put(42, person))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining(message)
                .hasCauseInstanceOf(QueryException.class)
                .hasStackTraceContaining(message);

        assertThat(map.size()).isEqualTo(6);
    }

    @Test
    public void testDestroy() {
        HazelcastInstance client = client();
        IMap<Integer, Person> map = client.getMap(tableName);
        map.loadAll(false);

        map.destroy();

        Row row = new Row("__map-store." + tableName);
        List<Row> rows = Collections.singletonList(row);
        assertTrueEventually(() -> assertDoesNotContainRow(client, "SHOW MAPPINGS", rows), 30);
    }

    /**
     * Regression test for https://github.com/hazelcast/hazelcast/issues/22567
     */
    @Test(timeout = 180_000L)
    @Category(NightlyTest.class)
    public void testClear() {
        HazelcastInstance client = client();
        IMap<Integer, Person> map = client.getMap(tableName);

        for (int i = 0; i < 10; i++) {
            logger.info("Iteration " + i);
            Map<Integer, Person> toInsert = new HashMap<>();
            for (int j = 0; j < 10_000; j++) {
                toInsert.put(j, new Person(j, "name-" + j));
            }
            map.putAll(toInsert);
            logger.info("Inserted 10k items");
            map.clear();
            logger.info("Cleared map");
        }
    }

    @Test
    public void testInstanceShutdown() throws Exception {
        HazelcastInstance client = client();
        IMap<Integer, Person> map = client.getMap(tableName);

        // create another member
        logger.info("Starting third member instance");
        HazelcastInstance hz3 = factory().newHazelcastInstance(memberConfig);

        HazelcastInstance[] instances = new HazelcastInstance[]{instances()[0], instances()[1], hz3};
        assertClusterSizeEventually(3, instances);
        waitAllForSafeState(instances);
        logger.info("Third member instance started with name " + hz3.getName());

        ExceptionRecorder recorder = new ExceptionRecorder(hz3, Level.WARNING);
        // fill the map with some values so each member gets some items
        Integer itemSize = 1000;
        logger.info("Putting data into the IMap");
        for (int i = 1; i < itemSize; i++) {
            map.put(i, new Person(i, "name-" + i));
        }
        logger.info("Putting data into the IMap finished");

        // Ensure that all put operations are inserted into the DB. Otherwise, we may get
        // HazelcastSqlException: Hazelcast instance is not active! from the SqlService
        // when we shut down the hz3 instance
        assertEqualsEventually(() -> {
                    List<Row> rows = jdbcRows("SELECT COUNT(*) FROM " + tableName);
                    Object[] values = rows.get(0).getValues();
                    return (Long) values[0];
                }, itemSize.longValue()
        );

        logger.info("Shutting down hz3");
        // shutdown the member - this will call destroy on the MapStore on this member,
        // which should not drop the mapping in this case
        hz3.shutdown();
        // Ensure that members have detected hz3 has shut down
        assertClusterSizeEventually(2, instances());
        // Ensure that the client has detected hz3 has shut down
        assertClusterSizeEventually(2, client);
        logger.info("Hz3 was shut down");

        // The new item should still be loadable via the mapping
        executeJdbc("INSERT INTO " + tableName + " VALUES(1000, 'name-1000')");
        logger.info("Executed the INSERT query");
        Person p = map.get(itemSize);
        assertThat(p.getId()).isEqualTo(itemSize);
        assertThat(p.getName()).isEqualTo("name-" + itemSize);

        for (Throwable throwable : recorder.exceptionsLogged()) {
            assertThat(throwable).hasMessageNotContaining("HazelcastSqlException: The Jet SQL job failed: "
                    + "Hazelcast instance is not active!");
        }
    }
}
