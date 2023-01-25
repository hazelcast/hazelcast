package com.hazelcast.jet.mongodb.sql;

import com.hazelcast.jet.mongodb.sql.FieldResolver.DbField;
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

import java.util.HashMap;
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
            Map<String, DbField> fields = resolver.readFields(readOpts);
            assertThat(fields).containsOnlyKeys("firstName", "lastName", "birthYear");
            assertThat(fields.get("lastName").columnTypeName).isEqualTo(BsonType.STRING);
            assertThat(fields.get("birthYear").columnTypeName).isEqualTo(BsonType.INT32);
        }
    }

    @Test
    public void testResolvesFieldsViaSample() {
        try (MongoClient client = MongoClients.create(mongoContainer.getConnectionString())) {
            String databaseName = "testDatabase";
            String collectionName = "people_2";
            MongoDatabase testDatabase = client.getDatabase(databaseName);
            MongoCollection<Document> collection = testDatabase.getCollection(collectionName);

            collection.insertOne(new Document("firstName", "Tomasz").append("lastName", "Gawęda").append("birthYear", 1992));

            FieldResolver resolver = new FieldResolver();

            Map<String, String> readOpts = new HashMap<>();
            readOpts.put("connectionString", mongoContainer.getConnectionString());
            readOpts.put("database", databaseName);
            readOpts.put("collection", collectionName);
            Map<String, DbField> fields = resolver.readFields(readOpts);
            assertThat(fields).containsOnlyKeys("_id", "firstName", "lastName", "birthYear");
            assertThat(fields.get("lastName").columnTypeName).isEqualTo(BsonType.STRING);
            assertThat(fields.get("birthYear").columnTypeName).isEqualTo(BsonType.INT32);
        }
    }

}