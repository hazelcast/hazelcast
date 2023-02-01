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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MongoDBContainer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class FieldResolverTest {
    static final String TEST_MONGO_VERSION = System.getProperty("test.mongo.version", "6.0.3");
    @ClassRule
    public static MongoDBContainer mongoContainer = new MongoDBContainer("mongo:" + TEST_MONGO_VERSION);

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
                            "      }\n" +
                            "    }\n" +
                            "  }\n"
            ));
            options.validationOptions(validationOptions);
            testDatabase.createCollection(collectionName, options);

            FieldResolver resolver = new FieldResolver();

            Map<String, String> readOpts = new HashMap<>();
            readOpts.put("connectionString", mongoContainer.getConnectionString());
            readOpts.put("database", databaseName);
            readOpts.put("collection", collectionName);
            Map<String, DocumentField> fields = resolver.readFields(readOpts, false);
            assertThat(fields).containsOnlyKeys("firstName", "lastName", "birthYear");
            assertThat(fields.get("lastName").columnType).isEqualTo(BsonType.STRING);
            assertThat(fields.get("birthYear").columnType).isEqualTo(BsonType.INT32);
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
                    .append("birthYear", 1992));

            FieldResolver resolver = new FieldResolver();

            Map<String, String> readOpts = new HashMap<>();
            readOpts.put("connectionString", mongoContainer.getConnectionString());
            readOpts.put("database", databaseName);
            readOpts.put("collection", collectionName);
            Map<String, DocumentField> fields = resolver.readFields(readOpts, false);
            assertThat(fields).containsOnlyKeys("_id", "firstName", "lastName", "birthYear");
            assertThat(fields.get("lastName").columnType).isEqualTo(BsonType.STRING);
            assertThat(fields.get("birthYear").columnType).isEqualTo(BsonType.INT32);
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

            FieldResolver resolver = new FieldResolver();

            Map<String, String> readOpts = new HashMap<>();
            readOpts.put("connectionString", mongoContainer.getConnectionString());
            readOpts.put("database", databaseName);
            readOpts.put("collection", collectionName);
            List<MappingField> fields = resolver.resolveFields(readOpts, Collections.emptyList(), false);
            assertThat(fields).contains(
                    new MappingField("_id", QueryDataType.VARCHAR).setPrimaryKey(true),
                    new MappingField("firstName", QueryDataType.VARCHAR).setPrimaryKey(false),
                    new MappingField("lastName", QueryDataType.VARCHAR).setPrimaryKey(false),
                    new MappingField("birthYear", QueryDataType.INT).setPrimaryKey(false)
            );
        }
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

            FieldResolver resolver = new FieldResolver();

            Map<String, String> readOpts = new HashMap<>();
            readOpts.put("connectionString", mongoContainer.getConnectionString());
            readOpts.put("database", databaseName);
            readOpts.put("collection", collectionName);
            List<MappingField> fields = resolver.resolveFields(readOpts, Arrays.asList(
                    new MappingField("id", QueryDataType.VARCHAR).setExternalName("_id"),
                    new MappingField("birthYear", QueryDataType.BIGINT)
            ), false);
            assertThat(fields).contains(
                    new MappingField("id", QueryDataType.VARCHAR).setPrimaryKey(true).setExternalName("_id"),
                    new MappingField("birthYear", QueryDataType.BIGINT).setExternalName("birthYear").setPrimaryKey(false)
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

            FieldResolver resolver = new FieldResolver();

            Map<String, String> readOpts = new HashMap<>();
            readOpts.put("connectionString", mongoContainer.getConnectionString());
            readOpts.put("database", databaseName);
            readOpts.put("collection", collectionName);
            try {
                resolver.resolveFields(readOpts, Collections.singletonList(
                        new MappingField("id", QueryDataType.MAP).setExternalName("_id")
                ), false);
            } catch (IllegalStateException e) {
                assertThat(e.getMessage()).isEqualTo("Type MAP of field id does not match db type VARCHAR");
            }
        }
    }

}
