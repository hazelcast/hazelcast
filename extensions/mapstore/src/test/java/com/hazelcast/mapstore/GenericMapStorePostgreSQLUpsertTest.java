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
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.jdbc.PostgresDatabaseProvider;
import org.example.Person;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.hazelcast.mapstore.GenericMapStore.EXTERNAL_REF_ID_PROPERTY;
import static com.hazelcast.mapstore.GenericMapStore.TYPE_NAME_PROPERTY;
import static org.assertj.core.util.Lists.newArrayList;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class GenericMapStorePostgreSQLUpsertTest extends JdbcSqlTestSupport {

    private String tableName;

    @BeforeClass
    public static void beforeClass() {
        databaseProvider = new PostgresDatabaseProvider();
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
        // Create a table and insert some Person objects
        tableName = randomTableName();
        createTable(tableName);
        insertItems(tableName, 1);

        // Add MapConfig for the table
        MapConfig mapConfig = new MapConfig(tableName);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setClassName(GenericMapStore.class.getName());
        mapStoreConfig.setProperty(EXTERNAL_REF_ID_PROPERTY, TEST_DATABASE_REF);
        mapStoreConfig.setProperty(TYPE_NAME_PROPERTY, "org.example.Person");
        mapConfig.setMapStoreConfig(mapStoreConfig);
        instance().getConfig().addMapConfig(mapConfig);
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
    public void testPutAllWhenExists() {
        HazelcastInstance client = client();
        IMap<Integer, Person> map = client.getMap(tableName);

        Map<Integer, Person> putMap = new LinkedHashMap<>();
        putMap.put(42, new Person(42, "name-42"));
        putMap.put(0, new Person(0, "updated"));
        putMap.put(44, new Person(44, "name-44"));
        putMap.put(45, new Person(45, "name-45"));
        map.putAll(putMap);

        assertJdbcRowsAnyOrder(tableName,
                new Row(0, "updated"),
                new Row(42, "name-42"),
                new Row(44, "name-44"),
                new Row(45, "name-45")
        );

        putMap.put(42, new Person(42, "42"));
        putMap.put(0, new Person(0, "0"));
        putMap.put(44, new Person(44, "44"));
        putMap.put(45, new Person(45, "45"));
        map.putAll(putMap);

        assertJdbcRowsAnyOrder(tableName,
                new Row(0, "0"),
                new Row(42, "42"),
                new Row(44, "44"),
                new Row(45, "45")
        );
    }

}
