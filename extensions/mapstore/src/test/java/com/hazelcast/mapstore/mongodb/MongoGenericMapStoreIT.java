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
package com.hazelcast.mapstore.mongodb;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.FilteringClassLoader;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.mongodb.compact.ObjectIdCompactSerializer;
import com.hazelcast.map.IMap;
import com.hazelcast.mapstore.GenericMapStore;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationOptions;
import org.bson.BsonDocument;
import org.bson.Document;
import org.example.PersonWithId;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MongoDBContainer;

import java.util.ArrayList;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.jet.mongodb.impl.Mappers.defaultCodecRegistry;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.newArrayList;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class MongoGenericMapStoreIT extends SimpleTestInClusterSupport {
    static final String TEST_MONGO_VERSION = System.getProperty("test.mongo.version", "6.0.3");

    public static MongoDBContainer mongoContainer = new MongoDBContainer("mongo:" + TEST_MONGO_VERSION);

    private static final String TEST_DATABASE_REF = "test-database-ref";
    private static MongoClient mongoClient;
    private static MongoDatabase database;
    private String tableName;

    @BeforeClass
    public static void beforeClass() {
        assumeDockerEnabled();
        mongoContainer.start();
        String connectionString = mongoContainer.getConnectionString();
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(randomName()).withCodecRegistry(defaultCodecRegistry());

        // Need to set filtering class loader so the members don't deserialize into class but into GenericRecord
        CompactSerializationConfig compactSerializationConfig =
                new CompactSerializationConfig().addSerializer(new ObjectIdCompactSerializer());
        SerializationConfig serializationConfig =
                new SerializationConfig().setCompactSerializationConfig(compactSerializationConfig);
        Config memberConfig = smallInstanceConfig()
                // Need to set filtering class loader so the members don't deserialize into class but into GenericRecord
                .setClassLoader(new FilteringClassLoader(newArrayList("org.example"), null))
                .addDataConnectionConfig(
                        new DataConnectionConfig(TEST_DATABASE_REF)
                                .setType("Mongo")
                                .setShared(false)
                                .setProperty("connectionString", connectionString)
                                .setProperty("database", database.getName())
                )
                .setSerializationConfig(serializationConfig);

        ClientConfig clientConfig = new ClientConfig()
                .setSerializationConfig(serializationConfig)
                .setClassLoader(new FilteringClassLoader(newArrayList("org.example"), null));

        initializeWithClient(2, memberConfig, clientConfig);
    }

    @AfterClass
    public static void afterClass() {
        closeResource(mongoClient);
        closeResource(mongoContainer);
    }

    @Before
    public void setUp() {
        tableName = randomName();
        createCollection(tableName);
        database.getCollection(tableName).insertOne(new Document("personId", 1).append("name", "name-1"));

        MapConfig mapConfig = new MapConfig(tableName);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setClassName(GenericMapStore.class.getName());
        mapStoreConfig.setProperty("data-connection-ref", TEST_DATABASE_REF);
        mapStoreConfig.setProperty("type-name", "org.example.PersonWithId");
        mapStoreConfig.setProperty("id-column", "personId");
        mapConfig.setMapStoreConfig(mapStoreConfig);
        instance().getConfig().addMapConfig(mapConfig);
    }

    private void createCollection(String collectionName) {
        CreateCollectionOptions options = new CreateCollectionOptions();
        ValidationOptions validationOptions = new ValidationOptions();
        validationOptions.validator(BsonDocument.parse(
                "{\n" +
                        "    $jsonSchema: {\n" +
                        "      bsonType: \"object\",\n" +
                        "      title: \"Object Validation\",\n" +
                        "      properties: {" +
                        "        \"personId\": { \"bsonType\": \"int\" },\n" +
                        "        \"name\": { \"bsonType\": \"string\" }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n"
        ));
        options.validationOptions(validationOptions);
        database.createCollection(collectionName, options);
    }

    @Test
    public void testGet() {
        HazelcastInstance client = client();
        IMap<Integer, PersonWithId> map = client.getMap(tableName);

        PersonWithId p = map.get(1);
        assertThat(p.getPersonId()).isEqualTo(1);
        assertThat(p.getName()).isEqualTo("name-1");
    }

    @Test
    public void testPut() {
        HazelcastInstance client = client();
        IMap<Integer, PersonWithId> map = client.getMap(tableName);

        map.put(42, new PersonWithId(42, "name-42"));

        assertMongoRowsAnyOrder(tableName,
                new PersonWithId(1, "name-1"),
                new PersonWithId(42, "name-42")
        );
    }

    @Test
    public void testPutWhenExists() {
        HazelcastInstance client = client();
        IMap<Integer, PersonWithId> map = client.getMap(tableName);

        assertMongoRowsAnyOrder(tableName,
                new PersonWithId(1, "name-1")
        );

        map.put(1, new PersonWithId(1, "updated"));

        assertMongoRowsAnyOrder(tableName,
                new PersonWithId(1, "updated")
        );
    }

    @Test
    public void testRemove() {
        HazelcastInstance client = client();
        IMap<Integer, PersonWithId> map = client.getMap(tableName);

        assertThat(database.getCollection(tableName).countDocuments()).isEqualTo(1);

        Integer key = map.keySet().iterator().next();
        map.remove(key);

        assertThat(database.getCollection(tableName).countDocuments()).isZero();
    }

    private void assertMongoRowsAnyOrder(String tableName, PersonWithId... p) {
        MongoCollection<Document> collection =
                database.getCollection(tableName).withCodecRegistry(defaultCodecRegistry());
        ArrayList<PersonWithId> list = collection.find(PersonWithId.class).into(new ArrayList<>());
        assertThat(list).containsExactlyInAnyOrder(p);
    }
}
