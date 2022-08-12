/*
 * Copyright 2021 Hazelcast Inc.
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
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.example.Person;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.mapstore.GenericMapStore.EXTERNAL_REF_ID_PROPERTY;
import static com.hazelcast.mapstore.GenericMapStore.TYPE_NAME_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.newArrayList;

public class GenericMapStoreIntegrationTest extends JdbcSqlTestSupport {

    private String tableName;

    @BeforeClass
    public static void beforeClass() throws Exception {
        databaseProvider = new H2DatabaseProvider();
        dbConnectionUrl = databaseProvider.createDatabase(JdbcSqlTestSupport.class.getName());

        Config config = smallInstanceConfig();
        config.getSerializationConfig().getCompactSerializationConfig().setEnabled(true);
        // Need to set filtering class loader so the members don't deserialize into class but into GenericRecord
        config.setClassLoader(new FilteringClassLoader(newArrayList("org.example"), null));

        config.addExternalDataStoreConfig(
                new ExternalDataStoreConfig(TEST_DATABASE_REF)
                        .setClassName(JdbcDataStoreFactory.class.getName())
                        .setProperty("jdbcUrl", dbConnectionUrl)
        );


        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().getCompactSerializationConfig().setEnabled(true);

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

        assertThat(jdbcRows(tableName)).containsOnly(
                new Row(0, "name-0"),
                new Row(42, "name-42")
        );
    }

    @Test
    public void testPutWhenExists() {
        HazelcastInstance client = client();
        IMap<Integer, Person> map = client.getMap(tableName);

        map.put(0, new Person(0, "updated"));

        assertThat(jdbcRows(tableName)).containsOnly(
                new Row(0, "updated")
        );
    }

    @Test
    public void testRemove() throws Exception {
        HazelcastInstance client = client();
        IMap<Integer, Person> map = client.getMap(tableName);

        assertThat(jdbcRows(tableName)).hasSize(1);

        map.remove(0);

        assertThat(jdbcRows(tableName)).isEmpty();
    }
}
