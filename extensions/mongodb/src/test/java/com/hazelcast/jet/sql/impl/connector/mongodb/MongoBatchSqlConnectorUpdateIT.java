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

import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationOptions;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;

import static com.mongodb.client.model.Filters.eq;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class})
public class MongoBatchSqlConnectorUpdateIT extends MongoSqlIT {

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
    public void updatesMongo_allHardcoded() {
        testUpdatesMongo("update " + collectionName + " set firstName = 'Han', jedi=false, lastName='Solo' " +
                        "where jedi=true or firstName = 'Han'");
    }

    @Test
    public void updatesMongo_setParametrized() {
        testUpdatesMongo("update " + collectionName + " set firstName = 'Han', lastName = ?, jedi=false " +
                        "where jedi=true or firstName = 'Han'", "Solo");
    }

    @Test
    public void updatesMongo_whereParametrized() {
        testUpdatesMongo("update " + collectionName + " set firstName = 'Han', lastName = 'Solo', jedi=false " +
                        "where jedi=true or firstName = ?", "Han");
    }

    @Test
    public void updatesMongo_allParametrized() {
        testUpdatesMongo("update " + collectionName + " set firstName = ?, lastName = ?, jedi=? " +
                        "where firstName = ?", "Han", "Solo", false, "temp");
    }

    @Test
    public void updatesMongo_whereWithReference() {
        testUpdatesMongo("update " + collectionName + " set firstName = ?, lastName = ?, jedi=? " +
                        "where firstName = lastName", "Han", "Solo", false);
    }

    public void testUpdatesMongo(String sql, Object... args) {
        createMapping(includeIdInMapping, idFirstInMapping);

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "temp").append("lastName", "temp")
                                                              .append("counter", 1).append("jedi", true));

        execute(sql, args);

        ArrayList<Document> list = collection.find().into(new ArrayList<>());
        assertThat(list).hasSize(1);
        Document item = list.get(0);
        assertEquals("Han", item.getString("firstName"));
        assertEquals("Solo", item.getString("lastName"));
        assertEquals(false, item.getBoolean("jedi"));
    }

    @Test
    public void updatesMongo_multipleRows() {
        createMapping(includeIdInMapping, idFirstInMapping);

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "temp").append("lastName", "temp").append("jedi", true));
        collection.insertOne(new Document("firstName", "temp2").append("lastName", "temp2").append("jedi", true));

        execute("update " + collectionName + " set firstName = lastName, lastName = ?, jedi=? " +
                        "where firstName = ?", "Solo", false, "temp");

        ArrayList<Document> list = collection.find(eq("firstName", "temp"))
                                             .into(new ArrayList<>());
        assertEquals(1, list.size());
        Document item = list.get(0);
        assertEquals("temp", item.getString("firstName"));
        assertEquals("Solo", item.getString("lastName"));
        assertEquals(false, item.getBoolean("jedi"));

        list = collection.find(eq("lastName", "temp2"))
                                             .into(new ArrayList<>());
        assertThat(list).hasSize(1);
    }

    @Test
    public void updatesMongo_multipleRows_unsupportedExprInFilter() {
        createMapping(includeIdInMapping, idFirstInMapping);

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "temp").append("lastName", "temp").append("jedi", true));
        collection.insertOne(new Document("firstName", "temp2").append("lastName", "temp2").append("jedi", false));

        execute("update " + collectionName + " set firstName = lastName, lastName = ?, jedi=? " +
                        "where cast(jedi as varchar) = ?", "Solo", false, "true");

        ArrayList<Document> list = collection.find(eq("lastName", "Solo"))
                                             .into(new ArrayList<>());
        assertEquals(1, list.size());
        Document item = list.get(0);
        assertEquals("temp", item.getString("firstName"));
        assertEquals("Solo", item.getString("lastName"));
        assertEquals(false, item.getBoolean("jedi"));

        list = collection.find(eq("lastName", "temp2"))
                                             .into(new ArrayList<>());
        assertThat(list).hasSize(1);
    }

    @Test
    public void updatesMongo_multipleRows_unsupportedExprInFilterAndValue() {
        createMapping(includeIdInMapping, idFirstInMapping);

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "temp").append("lastName", "temp")
                                                              .append("age", 20)
                                                              .append("jedi", true));
        collection.insertOne(new Document("firstName", "temp2").append("lastName", "temp2")
                                                               .append("age", 20)
                                                               .append("jedi", false));

        execute("update " + collectionName + " set firstName = cast(jedi as varchar), lastName = ?, jedi=? " +
                "where cast(jedi as varchar) = ?", "Solo", false, "true");

        ArrayList<Document> list = collection.find(eq("lastName", "Solo"))
                .into(new ArrayList<>());
        assertEquals(1, list.size());
        Document item = list.get(0);
        assertEquals("true", item.getString("firstName"));
        assertEquals("Solo", item.getString("lastName"));
        assertEquals(false, item.getBoolean("jedi"));

        list = collection.find(eq("lastName", "temp2"))
                .into(new ArrayList<>());
        assertThat(list).hasSize(1);
    }

    @Test
    public void testUpdatesMongo_multipleRows_unsupportedExprInValue() {
        createMapping(includeIdInMapping, idFirstInMapping);

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "temp").append("lastName", "temp").append("jedi", true));
        collection.insertOne(new Document("firstName", "temp2").append("lastName", "temp2").append("jedi", false));

        execute("update " + collectionName + " set firstName = cast(jedi as varchar), lastName = ?, jedi=? " +
                "where jedi = ?", "Solo", false, true);

        ArrayList<Document> list = collection.find(eq("lastName", "Solo"))
                .into(new ArrayList<>());
        assertEquals(1, list.size());
        Document item = list.get(0);
        assertEquals("true", item.getString("firstName"));
        assertEquals("Solo", item.getString("lastName"));
        assertEquals(false, item.getBoolean("jedi"));

        list = collection.find(eq("lastName", "temp2"))
                .into(new ArrayList<>());
        assertThat(list).hasSize(1);
    }

    @Test
    public void updatesMongo_noFailOnNoUpdates() {
        createMapping(includeIdInMapping, idFirstInMapping);

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "temp").append("lastName", "temp").append("jedi", true));


        execute("update " + collectionName + " set lastName = 'Solo' where firstName = 'NOT_EXIST'");

        ArrayList<Document> list = collection.find()
                                             .into(new ArrayList<>());
        assertEquals(1, list.size());
        Document item = list.get(0);
        assertEquals("temp", item.getString("lastName"));
    }

    @Test
    public void updatesMongo_whenCustomPK() {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "temp").append("lastName", "temp").append("jedi", true)
                .append("myPK_ext", 1337));

        execute("CREATE MAPPING " + collectionName
                + " ("
                + " myPK INT external name myPK_ext, "
                + " firstName VARCHAR, "
                + " lastName VARCHAR, "
                + " jedi BOOLEAN "
                + ") "
                + "DATA CONNECTION testMongo "
                + "OPTIONS ('idColumn' = 'myPK')");
        execute("update " + collectionName + " set firstName = ?, lastName = ?, jedi=? " +
                "where firstName = ?", "Han", "Solo", false, "temp");

        ArrayList<Document> list = collection.find(eq("firstName", "Han"))
                                             .into(new ArrayList<>());
        assertEquals(1, list.size());
        Document item = list.get(0);
        assertEquals("Han", item.getString("firstName"));
        assertEquals("Solo", item.getString("lastName"));
        assertEquals(false, item.getBoolean("jedi"));
        assertEquals(1337, item.getInteger("myPK_ext").intValue());
    }

    @Test
    public void updatesMongo_whenCustomPK_ensureUsage() {
        // ensure that custom PK is used for updates instead of _id
        // Custom PK field is not unique on purpose, if it is used, both records will be updates
        // which is expected result.
        // To force separate update step we use unsupported predicate in where.
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "temp").append("lastName", "temp").append("jedi", true)
                .append("myPK_ext", 1337));
        collection.insertOne(new Document("firstName", "temp").append("lastName", "temp2").append("jedi", false)
                .append("myPK_ext", 1337));
        assertThat(collection.countDocuments()).isEqualTo(2);

        execute("CREATE MAPPING " + collectionName
                + " ("
                + " myPK INT external name myPK_ext, "
                + " firstName VARCHAR, "
                + " lastName VARCHAR, "
                + " jedi BOOLEAN "
                + ") "
                + "DATA CONNECTION testMongo "
                + "OPTIONS ('idColumn' = 'myPK')");
        execute("update " + collectionName + " set firstName = ?, lastName = ?, jedi=? " +
                "where firstName = ? and cast(jedi as varchar)=?", "Han", "Solo", false, "temp", "true");

        assertThat(collection.countDocuments()).isEqualTo(2);
        ArrayList<Document> list = collection.find(eq("firstName", "Han"))
                .into(new ArrayList<>());
        assertEquals(2, list.size());
    }

    @Test
    public void updatesMongo_whenCustomPKNotFirst() {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "temp").append("lastName", "temp").append("jedi", true)
                .append("myPK_ext", 1337));

        execute("CREATE MAPPING " + collectionName
                + " ("
                + " firstName VARCHAR, "
                + " myPK INT external name myPK_ext, "
                + " lastName VARCHAR, "
                + " jedi BOOLEAN "
                + ") "
                + "DATA CONNECTION testMongo "
                + "OPTIONS ('idColumn' = 'myPK')");
        execute("update " + collectionName + " set firstName = ?, lastName = ?, jedi=? " +
                "where firstName = ?", "Han", "Solo", false, "temp");

        ArrayList<Document> list = collection.find(eq("firstName", "Han"))
                .into(new ArrayList<>());
        assertEquals(1, list.size());
        Document item = list.get(0);
        assertEquals("Han", item.getString("firstName"));
        assertEquals("Solo", item.getString("lastName"));
        assertEquals(false, item.getBoolean("jedi"));
        assertEquals(1337, item.getInteger("myPK_ext").intValue());
    }

    @Test
    public void updatesMongo_whenCustomPKNotFirst_unsupportedExpr() {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "temp").append("lastName", "temp").append("jedi", true)
                .append("myPK_ext", 1337));

        execute("CREATE MAPPING " + collectionName
                + " ("
                + " firstName VARCHAR, "
                + " myPK INT external name myPK_ext, "
                + " lastName VARCHAR, "
                + " jedi BOOLEAN "
                + ") "
                + "DATA CONNECTION testMongo "
                + "OPTIONS ('idColumn' = 'myPK')");
        execute("update " + collectionName + " set firstName = ?, lastName = ?, jedi=? " +
                "where cast(jedi as varchar) = ?", "Han", "Solo", false, "true");

        ArrayList<Document> list = collection.find(eq("firstName", "Han"))
                .into(new ArrayList<>());
        assertEquals(1, list.size());
        Document item = list.get(0);
        assertEquals("Han", item.getString("firstName"));
        assertEquals("Solo", item.getString("lastName"));
        assertEquals(false, item.getBoolean("jedi"));
        assertEquals(1337, item.getInteger("myPK_ext").intValue());
    }

    @Test
    public void updatesMongo_allRows() {
        createMapping(includeIdInMapping, idFirstInMapping);

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "temp").append("lastName", "temp").append("jedi", true));
        collection.insertOne(new Document("firstName", "temp2").append("lastName", "temp2").append("jedi", false));

        execute("update " + collectionName + " set lastName = 'Solo'");

        ArrayList<Document> list = collection.find()
                                             .into(new ArrayList<>());
        assertEquals(2, list.size());
        assertThat(list).extracting(i -> i.getString("lastName")).contains("Solo", "Solo");
    }

    @Test
    public void updatesMongo_usingNonEquality() {
        createMapping(includeIdInMapping, idFirstInMapping);

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "temp").append("lastName", "temp").append("jedi", true));
        collection.insertOne(new Document("firstName", "temp2").append("lastName", "temp2").append("jedi", false));

        execute("update " + collectionName + " set lastName = 'Solo' where firstName <> 'temp'");

        ArrayList<Document> list = collection.find(eq("firstName", "temp2"))
                                             .into(new ArrayList<>());
        assertEquals(1, list.size());
        assertThat(list).extracting(i -> i.getString("lastName")).contains("Solo", "Solo");
    }

    @Test
    public void updatesMongo_usingGT_LT() {
        updatesMongo_using("age > 30 and age < 50");
    }

    @Test
    public void updatesMongo_usingGTE_LTE() {
        updatesMongo_using("age >= 40 and age <= 50");
    }

    @Test
    public void updatesMongo_usingNonEq() {
        updatesMongo_using("age <> 20");
    }

    private void updatesMongo_using(String expr) {
        createMapping(includeIdInMapping, idFirstInMapping);

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "temp").append("age", 20).append("lastName", "Teamed"));
        collection.insertOne(new Document("firstName", "temp2").append("age", 40).append("lastName", "Teamed"));

        execute("update " + collectionName + " set lastName = 'Solo' where " + expr);

        assertThat(collection.find(eq("firstName", "temp2"))
                             .first().getString("lastName")).isEqualTo("Solo");
        assertThat(collection.find(eq("firstName", "temp"))
                             .first().getString("lastName")).isNotEqualTo("Solo");
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
