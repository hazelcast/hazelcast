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

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationOptions;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class MongoBatchSqlConnectorDeleteIT extends MongoSqlIT {

    @Parameterized.Parameter(0)
    public boolean includeIdInMapping;
    @Parameterized.Parameter(1)
    public boolean idFirstInMapping;

    @Parameterized.Parameters(name = "includeIdInMapping:{0}, idFirstInMapping:{1}")
    public static Object[][] parameters() {
        return new Object[][]{
                { true, true },
                { true, false },
                { false, true }
        };
    }

    @Test
    public void deletes_inserted_item() {
        createMapping(includeIdInMapping, idFirstInMapping);

        MongoCollection<Document> collection = database.getCollection(collectionName);
        ObjectId objectId = ObjectId.get();
        collection.insertOne(new Document("_id", objectId).append("firstName", "temp").append("lastName", "temp")
                                                          .append("jedi", true));
        collection.insertOne(new Document("_id", ObjectId.get()).append("firstName", "temp2").append("lastName", "temp2")
                                                          .append("jedi", true));
        collection.insertOne(new Document("_id", ObjectId.get()).append("firstName", "temp3").append("lastName", "temp3")
                                                          .append("jedi", true));

        execute("delete from " + collectionName + " where firstName = ?", "temp");
        ArrayList<Document> list = collection.find().into(new ArrayList<>());
        assertThat(list).hasSize(2);
        execute("delete from " + collectionName);
        list = collection.find().into(new ArrayList<>());
        assertThat(list).isEmpty();
    }

    @Test
    public void deletes_inserted_item_byId() {
        createMapping(true, idFirstInMapping);

        MongoCollection<Document> collection = database.getCollection(collectionName);
        ObjectId objectId = ObjectId.get();
        collection.insertOne(new Document("_id", objectId).append("firstName", "temp").append("lastName", "temp")
                                                          .append("jedi", true));
        collection.insertOne(new Document("_id", ObjectId.get()).append("firstName", "temp2").append("lastName", "temp2")
                                                          .append("jedi", true));
        collection.insertOne(new Document("_id", ObjectId.get()).append("firstName", "temp3").append("lastName", "temp3")
                                                          .append("jedi", true));

        execute("delete from " + collectionName + " where id = ?", objectId);
        ArrayList<Document> list = collection.find().into(new ArrayList<>());
        assertThat(list).hasSize(2);
        execute("delete from " + collectionName);
        list = collection.find().into(new ArrayList<>());
        assertThat(list).isEmpty();
    }

    @Test
    public void deletes_selfRef_in_where() {
        createMapping(includeIdInMapping, idFirstInMapping);

        MongoCollection<Document> collection = database.getCollection(collectionName);
        ObjectId objectId = ObjectId.get();
        collection.insertOne(new Document("_id", objectId).append("firstName", "someValue")
                                                          .append("lastName", "someValue")
                                                          .append("jedi", true));
        collection.insertOne(new Document("_id", ObjectId.get()).append("firstName", "someValue2")
                                                                .append("lastName", "someValue2")
                                                                .append("jedi", true));
        collection.insertOne(new Document("_id", ObjectId.get()).append("firstName", "someValue3")
                                                                .append("lastName", "otherValue")
                                                                .append("jedi", true));

        execute("delete from " + collectionName + " where firstName = lastName");
        ArrayList<Document> list = collection.find().into(new ArrayList<>());
        assertThat(list).hasSize(1);
        assertThat(list.get(0).getString("lastName")).isEqualTo("otherValue");
    }

    @Test
    public void deletes_unsupportedExpr() {
        createMapping(includeIdInMapping, idFirstInMapping);

        MongoCollection<Document> collection = database.getCollection(collectionName);
        ObjectId objectId = ObjectId.get();
        collection.insertOne(new Document("_id", objectId).append("firstName", "temp").append("lastName", "temp")
                                                          .append("jedi", true));

        execute("delete from " + collectionName + " where cast(jedi as varchar) = ?", "true");
        ArrayList<Document> list = collection.find().into(new ArrayList<>());
        assertThat(list).hasSize(0);
    }

    @Test
    public void deletes_all() {
        createMapping(includeIdInMapping, idFirstInMapping);

        MongoCollection<Document> collection = database.getCollection(collectionName);
        ObjectId objectId = ObjectId.get();
        collection.insertOne(new Document("_id", objectId).append("firstName", "temp").append("lastName", "temp")
                                                          .append("jedi", true));

        execute("delete from " + collectionName);
        ArrayList<Document> list = collection.find().into(new ArrayList<>());
        assertThat(list).hasSize(0);
    }

    @Test
    public void deletesMongo_usingGT_LT() {
        deletesMongo_using("age > 30 and age < 50");
    }

    @Test
    public void deletesMongo_usingGTE_LTE() {
        deletesMongo_using("age >= 40 and age <= 50");
    }

    @Test
    public void deletesMongo_usingNonEq() {
        deletesMongo_using("age <> 20");
    }

    private void deletesMongo_using(String expr) {
        createMapping(includeIdInMapping, idFirstInMapping);

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "temp").append("age", 20).append("lastName", "Teamed"));
        collection.insertOne(new Document("firstName", "temp2").append("age", 40).append("lastName", "Teamed"));

        execute("delete from " + collectionName + " where " + expr);

        List<Document> documents = collection.find().into(new ArrayList<>());
        assertThat(documents)
                .hasSize(1)
                .extracting(doc -> doc.getInteger("age")).containsExactly(20);
    }

    private void createMapping(boolean includeIdInMapping, boolean idFirst) {
        CreateCollectionOptions options = new CreateCollectionOptions();
        ValidationOptions validationOptions = new ValidationOptions();
        validationOptions.validator(BsonDocument.parse(
                "{\n" +
                        "    $jsonSchema: {\n" +
                        "      bsonType: \"object\",\n" +
                        "      properties: {" +
                        (includeIdInMapping ? "\"_id\": { \"bsonType\": \"objectId\" },\n" : "") +
                        "        \"firstName\": { \"bsonType\": \"string\" },\n" +
                        "        \"lastName\": { \"bsonType\": \"string\" },\n" +
                        "        \"jedi\": { \"bsonType\": \"bool\" },\n" +
                        "        \"age\": { \"bsonType\": \"int\" }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n"
        ));
        options.validationOptions(validationOptions);
        mongoClient.getDatabase(databaseName).createCollection(collectionName, options);
        if (idFirst) {
            execute("CREATE MAPPING " + collectionName + " external name \"" + databaseName + "\".\""
                        + collectionName + "\" \n("
                    + (includeIdInMapping ? " id OBJECT external name _id, " : "")
                    + " firstName VARCHAR, \n"
                    + " lastName VARCHAR, \n"
                    + " jedi BOOLEAN, \n"
                    + " age INTEGER \n"
                    + ") \n"
                    + "TYPE Mongo \n"
                    + "OPTIONS (\n"
                    + "    'connectionString' = '" + mongoContainer.getConnectionString() + "' \n"
                    + ")"
            );
        } else {
            execute("CREATE MAPPING " + collectionName + " external name \"" + databaseName + "\".\""
                       + collectionName + "\" \n("
                    + " firstName VARCHAR, \n"
                    + " lastName VARCHAR, \n"
                    + (includeIdInMapping ? " id OBJECT external name _id, " : "")
                    + " jedi BOOLEAN, \n"
                    + " age INTEGER \n"
                    + ") \n"
                    + "TYPE Mongo \n"
                    + "OPTIONS (\n"
                    + "    'connectionString' = '" + mongoContainer.getConnectionString() + "' \n"
                    + ")"
            );
        }
    }
}
