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
package com.hazelcast.jet.sql.impl.connector.mongodb;

import com.hazelcast.jet.sql.impl.connector.mongodb.FieldResolver.DocumentField;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationOptions;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MongoDBContainer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.OBJECT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class FieldResolverTest {
    static final String TEST_MONGO_VERSION = System.getProperty("test.mongo.version", "6.0.3");
    private static final MongoDBContainer mongoContainer = new MongoDBContainer("mongo:" + TEST_MONGO_VERSION);

    @BeforeClass
    public static void setUp() {
        assumeDockerEnabled();
        mongoContainer.start();
    }

    @AfterClass
    public static void tearDown() {
        if (mongoContainer != null) {
            mongoContainer.stop();
        }
    }

    @Test
    public void testResolvesFieldsViaSchema() {
        try (MongoClient client = MongoClients.create(mongoContainer.getConnectionString())) {
            String databaseName = "testDatabase";
            String collectionName = "people";
            MongoDatabase testDatabase = client.getDatabase(databaseName);

            CreateCollectionOptions options = new CreateCollectionOptions();
            ValidationOptions validationOptions = new ValidationOptions();
            validationOptions.validator(BsonDocument.parse(
                    "{\n" +
                    "    $jsonSchema: {\n" +
                            "      bsonType: \"object\",\n" +
                            "      title: \"Person Object Validation\",\n" +
                            "      required: [ \"firstName\", \"lastName\", \"birthYear\" ],\n" +
                            "      properties: {" +
                            "        \"firstName\": { \"bsonType\": \"string\" }\n" +
                            "        \"lastName\": { \"bsonType\": \"string\" }\n" +
                            "        \"birthYear\": { \"bsonType\": \"int\" }\n" +
                            "        \"title\": { \"enum\": [ \"Bsc\", \"Msc\", \"PhD\" ] }\n" +
                            "        \"intOrString\": { \"enum\": [ \"String\", 1 ] }\n" +
                            "        \"unionType\": { \"bsonType\": [ 'int', 'string' ] }\n" +
                            "      }\n" +
                            "    }\n" +
                            "  }\n"
            ));
            options.validationOptions(validationOptions);
            testDatabase.createCollection(collectionName, options);

            FieldResolver resolver = new FieldResolver(null);

            Map<String, String> readOpts = new HashMap<>();
            readOpts.put("connectionString", mongoContainer.getConnectionString());
            readOpts.put("database", databaseName);
            Map<String, DocumentField> fields = resolver.readFields(new String[]{databaseName, collectionName}, null,
                    readOpts, false);
            assertThat(fields)
                    .containsOnlyKeys("firstName", "lastName", "birthYear", "title", "unionType", "intOrString");
            assertThat(fields.get("lastName").columnType).isEqualTo(BsonType.STRING);
            assertThat(fields.get("birthYear").columnType).isEqualTo(BsonType.INT32);
            assertThat(fields.get("title").columnType).isEqualTo(BsonType.STRING);
            assertThat(fields.get("intOrString").columnType).isEqualTo(BsonType.DOCUMENT);
            assertThat(fields.get("unionType").columnType).isEqualTo(BsonType.DOCUMENT);
        }
    }


    @Test
    public void testResolvesFieldsViaSample() {
        try (MongoClient client = MongoClients.create(mongoContainer.getConnectionString())) {
            String databaseName = "testDatabase";
            String collectionName = "people_2";
            MongoDatabase testDatabase = client.getDatabase(databaseName);
            MongoCollection<Document> collection = testDatabase.getCollection(collectionName);

            collection.insertOne(new Document("firstName", "Tomasz")
                    .append("lastName", "Gawęda")
                    .append("birthYear", 1992)
                    .append("citizenship", new HashSet<>(singletonList("Polish")))
                    .append("citizenshipButList", singletonList("Polish"))
            );

            FieldResolver resolver = new FieldResolver(null);

            Map<String, String> readOpts = new HashMap<>();
            readOpts.put("connectionString", mongoContainer.getConnectionString());
            readOpts.put("database", databaseName);
            Map<String, DocumentField> fields = resolver.readFields(new String[]{databaseName, collectionName},
                    null, readOpts, false);
            assertThat(fields).containsOnlyKeys("_id", "firstName", "lastName", "birthYear", "citizenship",
                    "citizenshipButList");
            assertThat(fields.get("lastName").columnType).isEqualTo(BsonType.STRING);
            assertThat(fields.get("birthYear").columnType).isEqualTo(BsonType.INT32);
            assertThat(fields.get("citizenship").columnType).isEqualTo(BsonType.ARRAY);
            assertThat(fields.get("citizenshipButList").columnType).isEqualTo(BsonType.ARRAY);
        }
    }

    @Test
    public void testResolvesMappingFieldsViaSample() {
        try (MongoClient client = MongoClients.create(mongoContainer.getConnectionString())) {
            String databaseName = "testDatabase";
            String collectionName = "people_3";
            MongoDatabase testDatabase = client.getDatabase(databaseName);
            MongoCollection<Document> collection = testDatabase.getCollection(collectionName);

            collection.insertOne(new Document("firstName", "Tomasz")
                    .append("lastName", "Gawęda")
                    .append("birthYear", 1992));

            FieldResolver resolver = new FieldResolver(null);

            Map<String, String> readOpts = new HashMap<>();
            readOpts.put("connectionString", mongoContainer.getConnectionString());
            List<MappingField> fields = resolver.resolveFields(new String[]{databaseName, collectionName},
                    null, readOpts, emptyList(), false);
            assertThat(fields).contains(
                    fieldWithSameExternal("_id", OBJECT, BsonType.OBJECT_ID).setPrimaryKey(true),
                    fieldWithSameExternal("firstName", VARCHAR, BsonType.STRING),
                    fieldWithSameExternal("lastName", VARCHAR, BsonType.STRING),
                    fieldWithSameExternal("birthYear", INT, BsonType.INT32)
            );
        }
    }

    @Test
    public void testResolvesMappingFieldsViaSampleInStream() {
        try (MongoClient client = MongoClients.create(mongoContainer.getConnectionString())) {
            String databaseName = "testDatabase";
            String collectionName = "testResolvesMappingFieldsViaSampleInStream";
            MongoDatabase testDatabase = client.getDatabase(databaseName);
            MongoCollection<Document> collection = testDatabase.getCollection(collectionName);

            collection.insertOne(new Document("firstName", "Tomasz")
                    .append("lastName", "Gawęda")
                    .append("birthYear", 1992));

            FieldResolver resolver = new FieldResolver(null);

            Map<String, String> readOpts = new HashMap<>();
            readOpts.put("connectionString", mongoContainer.getConnectionString());
            List<MappingField> fields = resolver.resolveFields(new String[]{databaseName, collectionName},
                    null, readOpts, emptyList(), true);
            assertThat(fields).contains(
                    fieldWithSameExternal("resumeToken", VARCHAR, BsonType.STRING),
                    fieldWithSameExternal("operationType", VARCHAR, BsonType.STRING),
                    fieldWithSameExternal("fullDocument._id", OBJECT, BsonType.OBJECT_ID).setPrimaryKey(true),
                    fieldWithSameExternal("fullDocument.firstName", VARCHAR, BsonType.STRING),
                    fieldWithSameExternal("fullDocument.lastName", VARCHAR, BsonType.STRING),
                    fieldWithSameExternal("fullDocument.birthYear", INT, BsonType.INT32)
            );
        }
    }

    private static MappingField fieldWithSameExternal(String name, QueryDataType type, BsonType externalType) {
        return new MappingField(name, type, name).setPrimaryKey(false).setExternalType(externalType.name());
    }

    @Test
    public void testResolvesMappingFieldsViaSample_withUserFields() {
        try (MongoClient client = MongoClients.create(mongoContainer.getConnectionString())) {
            String databaseName = "testDatabase";
            String collectionName = "people_3";
            MongoDatabase testDatabase = client.getDatabase(databaseName);
            MongoCollection<Document> collection = testDatabase.getCollection(collectionName);

            collection.insertOne(new Document("firstName", "Tomasz")
                    .append("lastName", "Gawęda")
                    .append("birthYear", 1992));

            FieldResolver resolver = new FieldResolver(null);

            Map<String, String> readOpts = new HashMap<>();
            readOpts.put("connectionString", mongoContainer.getConnectionString());
            List<MappingField> fields = resolver.resolveFields(new String[]{databaseName, collectionName},
                    null, readOpts, Arrays.asList(
                    new MappingField("id", OBJECT).setExternalName("_id"),
                    new MappingField("birthYear", QueryDataType.BIGINT)
            ), false);
            assertThat(fields).contains(
                    new MappingField("id", OBJECT).setPrimaryKey(true).setExternalName("_id").setExternalType("OBJECT_ID"),
                    new MappingField("birthYear", QueryDataType.BIGINT)
                            .setExternalName("birthYear").setPrimaryKey(false).setExternalType("INT32")
            );
        }
    }

    @Test
    public void testResolvesMappingFieldsViaSample_wrongUserType() {
        try (MongoClient client = MongoClients.create(mongoContainer.getConnectionString())) {
            String databaseName = "testDatabase";
            String collectionName = "people_3";
            MongoDatabase testDatabase = client.getDatabase(databaseName);
            MongoCollection<Document> collection = testDatabase.getCollection(collectionName);

            collection.insertOne(new Document("firstName", "Tomasz")
                    .append("lastName", "Gawęda")
                    .append("birthYear", 1992));

            FieldResolver resolver = new FieldResolver(null);

            Map<String, String> readOpts = new HashMap<>();
            readOpts.put("connectionString", mongoContainer.getConnectionString());
            try {
                resolver.resolveFields(new String[]{databaseName, collectionName}, null, readOpts,
                        singletonList(new MappingField("id", QueryDataType.MAP).setExternalName("_id")), false);
            } catch (IllegalStateException e) {
                assertThat(e.getMessage()).isEqualTo("Type MAP of field id does not match db type OBJECT");
            }
        }
    }

    @Test
    public void testFailsOnNoDatabase() {
        String collectionName = "people_3";
        FieldResolver resolver = new FieldResolver(null);

        Map<String, String> readOpts = new HashMap<>();
        readOpts.put("connectionString", mongoContainer.getConnectionString());
        try {
            resolver.resolveFields(new String[]{collectionName}, null, readOpts, singletonList(
                    new MappingField("id", QueryDataType.MAP).setExternalName("_id")
            ), false);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("Database must be provided in the mapping or data connection.");
        }
    }

}
