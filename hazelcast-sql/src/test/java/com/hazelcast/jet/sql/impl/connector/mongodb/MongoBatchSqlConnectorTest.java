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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.EmptyStatement;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.logging.LogListener;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MongoDBContainer;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class MongoBatchSqlConnectorTest extends SqlTestSupport {
    private static final String TEST_MONGO_VERSION = System.getProperty("test.mongo.version", "6.0.3");

    @ClassRule
    public static final MongoDBContainer mongoContainer
            = new MongoDBContainer("mongo:" + TEST_MONGO_VERSION);
    private static final String DATABASE_NAME = "sqlConnectorTest";

    private static SqlService sqlService;
    private static MongoClient mongoClient;
    private static MongoDatabase database;

    @Rule
    public final TestName testName = new TestName();

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
        sqlService = instance().getSql();
        mongoClient = MongoClients.create(mongoContainer.getConnectionString());
        database = mongoClient.getDatabase(DATABASE_NAME);
    }

    @AfterClass
    public static void close() {
        mongoClient.close();
    }

    @Test
    public void readsFromMongo_withId_twoSteps() {
       readsFromMongo(true, true);
    }

    @Test
    public void readsFromMongo_withId_oneStep() {
       readsFromMongo(true, false);
    }

    @Test
    public void readsFromMongo_withoutId_twoSteps() {
       readsFromMongo(false, true);
    }
    @Test
    public void readsFromMongo_withoutId_oneStep() {
       readsFromMongo(false, false);
    }

    public void readsFromMongo(boolean includeIdInMapping, boolean forceTwoSteps) {
        final String collectionName = methodName();
        final String tableName = "people";
        final String connectionString = mongoContainer.getConnectionString();

        AtomicBoolean projectAndFilterFound = new AtomicBoolean(false);
        LogListener lookForProjectAndFilterStep = log -> {
            String message = log.getLogRecord().getMessage();
            if (message.contains("ProjectAndFilter")) {
                projectAndFilterFound.set(true);
            }
        };
        for (HazelcastInstance instance : instances()) {
            instance.getLoggingService().addLogListener(Level.FINE, lookForProjectAndFilterStep);
        }

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "Luke").append("lastName", "Skywalker").append("jedi", true));
        collection.insertOne(new Document("firstName", "Han").append("lastName", "Solo").append("jedi", false));
        collection.insertOne(new Document("firstName", "Anakin").append("lastName", "Skywalker").append("jedi", true));

        execute("CREATE MAPPING " + tableName
                + " ("
                + (includeIdInMapping ? " id VARCHAR external name _id, " : "")
                + " firstName VARCHAR, "
                + " lastName VARCHAR, "
                + " jedi BOOLEAN "
                + ") "
                + "TYPE MongoDB "
                + "OPTIONS ("
                + "    'connectionString' = '" + connectionString + "', "
                + "    'database' = '" + DATABASE_NAME + "', "
                + "    'collection' = '" + collectionName + "' "
                + ")");

        String force = forceTwoSteps ? " and cast(jedi as varchar) = 'true' " : "";
        assertRowsAnyOrder("select firstName, lastName from " + tableName
                        + " where lastName = ? and jedi=true" + force,
                singletonList("Skywalker"),
                asList(
                        new Row("Luke", "Skywalker"),
                        new Row("Anakin", "Skywalker")
                )
        );
        assertEquals(forceTwoSteps, projectAndFilterFound.get());
        for (HazelcastInstance instance : instances()) {
            instance.getLoggingService().removeLogListener(lookForProjectAndFilterStep);
        }
    }


    protected void execute(String sql, Object... arguments) {
        try (SqlResult ignored = sqlService.execute(sql, arguments)) {
            EmptyStatement.ignore(null);
        }
    }

    @Test
    public void insertsIntoMongo_parametrized_withId() {
        insertsIntoMongo(true, "insert into " + methodName() + "(jedi, firstName, lastName) values (?, 'Han', ?)",
                false, "Solo");
    }
    @Test
    public void insertsIntoMongo_parametrized_withoutId() {
        insertsIntoMongo(false, "insert into " + methodName() + "(jedi, firstName, lastName) " +
                "values (false, 'Han', 'Solo')");
    }

    public void insertsIntoMongo(boolean includeId, String sql, Object... args) {
        final String collectionName = methodName();

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "temp").append("lastName", "temp").append("jedi", true));

        createMapping(includeId);

        execute(sql, args);

        ArrayList<Document> list = collection.find(Filters.eq("firstName", "Han"))
                                             .into(new ArrayList<>());
        assertEquals(1, list.size());
        Document item = list.get(0);
        assertEquals("Han", item.getString("firstName"));
        assertEquals("Solo", item.getString("lastName"));
        assertEquals(false, item.getBoolean("jedi"));
    }

    @Test
    public void insertsIntoMongo_duplicate() {
        MongoCollection<Document> collection = database.getCollection(methodName());
        String insertedId =
                collection.insertOne(new Document("firstName", "temp").append("lastName", "temp").append("jedi",
                        true)).getInsertedId().asObjectId().getValue().toHexString();
        createMapping(true);
        assertThatThrownBy(() -> execute("insert into " + methodName() + " (id, firstName, jedi) values (?, ?, ?)",
                insertedId, "yolo", false))
                .hasRootCauseInstanceOf(MongoBulkWriteException.class)
                .hasMessageContaining("E11000 duplicate key error collection");
    }

    @Test
    public void updatesMongo_allHardcoded() {
        updatesMongo(true,
                "update " + methodName() + " set firstName = 'Han', lastName = 'Solo', jedi=false " +
                        "where jedi=true or firstName = 'Han'");
    }
    @Test
    public void updatesMongo_setParametrized() {
        updatesMongo(true,
                "update " + methodName() + " set firstName = 'Han', lastName = ?, jedi=false " +
                        "where jedi=true or firstName = 'Han'", "Solo");
    }
    @Test
    public void updatesMongo_whereParametrized() {
        updatesMongo(true,
                "update " + methodName() + " set firstName = 'Han', lastName = 'Solo', jedi=false " +
                        "where jedi=true or firstName = ?", "Han");
    }
    @Test
    public void updatesMongo_allParametrized() {
        updatesMongo(true,
                "update " + methodName() + " set firstName = ?, lastName = ?, jedi=? " +
                        "where firstName = ?", "Han", "Solo", false, "temp");
    }

    public void updatesMongo(boolean includeIdInMapping, String sql, Object... args) {
        final String collectionName = methodName();

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "temp").append("lastName", "temp").append("jedi", true));

        createMapping(includeIdInMapping);

        execute(sql, args);

        ArrayList<Document> list = collection.find(Filters.eq("firstName", "Han"))
                                             .into(new ArrayList<>());
        assertEquals(1, list.size());
        Document item = list.get(0);
        assertEquals("Han", item.getString("firstName"));
        assertEquals("Solo", item.getString("lastName"));
        assertEquals(false, item.getBoolean("jedi"));


    }

    @Test
    public void updatesMongo_noFailOnNoUpdates() {
        final String collectionName = methodName();

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "temp").append("lastName", "temp").append("jedi", true));

        createMapping(true);

        execute("update " + methodName() + " set lastName = 'Solo' where firstName = 'NOT_EXIST'");

        ArrayList<Document> list = collection.find(Filters.eq("firstName", "temp"))
                                             .into(new ArrayList<>());
        assertEquals(1, list.size());
        Document item = list.get(0);
        assertEquals("temp", item.getString("lastName"));
    }

    @Test
    public void sinkInto_allHardcoded_withId() {
        sinksIntoMongo(true, "sink into " + methodName() + " (firstName, lastName, jedi) values ('Leia', 'Organa', true)");
    }
    @Test
    public void sinkInto_allHardcoded_withoutId() {
        sinksIntoMongo(false, "sink into " + methodName() + " (firstName, lastName, jedi) values ('Leia', 'Organa', true)");
    }

    @Test
    public void sinkInto_oneParametrized_withId() {
        sinksIntoMongo(true, "sink into " + methodName() + " (firstName, lastName, jedi) values ('Leia', ?, true)",
                "Organa");
    }
    @Test
    public void sinkInto_oneParametrized_withoutId() {
        sinksIntoMongo(false, "sink into " + methodName() + " (firstName, lastName, jedi) values ('Leia', ?, true)",
                "Organa");
    }

    public void sinksIntoMongo(boolean includeId, String sql, Object... args) {
        final String collectionName = methodName();

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "temp").append("lastName", "temp").append("jedi", true));
        collection.insertOne(new Document("firstName", "temp2").append("lastName", "temp2").append("jedi", true));

        createMapping(includeId);

        execute(sql, args);
        ArrayList<Document> list = collection.find(Filters.eq("firstName", "Leia"))
                                             .into(new ArrayList<>());
        assertEquals(1, list.size());
        Document item = list.get(0);
        assertEquals("Leia", item.getString("firstName"));
        assertEquals("Organa", item.getString("lastName"));
        assertEquals(true, item.getBoolean("jedi"));
    }

    private void createMapping(boolean includeIdInMapping) {
        execute("CREATE MAPPING " + methodName()
                + " ("
                + (includeIdInMapping ? " id VARCHAR external name _id, " : "")
                + " firstName VARCHAR, "
                + " lastName VARCHAR, "
                + " jedi BOOLEAN "
                + ") "
                + "TYPE MongoDB "
                + "OPTIONS ("
                + "    'connectionString' = '" + mongoContainer.getConnectionString() + "', "
                + "    'database' = '" + DATABASE_NAME + "', "
                + "    'collection' = '" + methodName() + "' "
                + ")");
    }

    private String methodName() {
        return testName.getMethodName();
    }

}
