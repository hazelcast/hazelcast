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
import com.hazelcast.logging.LogListener;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class MongoBatchSqlConnectorTest extends MongoSqlTest {

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

        execute("CREATE MAPPING " + collectionName
                + " ("
                + (includeIdInMapping ? " id VARCHAR external name _id, " : "")
                + " firstName VARCHAR, "
                + " lastName VARCHAR, "
                + " jedi BOOLEAN "
                + ") "
                + "TYPE MongoDB "
                + "OPTIONS ("
                + "    'connectionString' = '" + connectionString + "', "
                + "    'database' = '" +  databaseName + "' "
                + ")");

        String force = forceTwoSteps ? " and cast(jedi as varchar) = 'true' " : "";
        assertRowsAnyOrder("select firstName, lastName from " + collectionName
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
    public void readWithTypeCoertion() {
        final String collectionName = methodName();

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "Luke").append("lastName", "Skywalker").append("jedi", "true"));

        createMapping(true);
        collection.insertOne(new Document("firstName", "Han").append("lastName", "Solo").append("jedi", "false"));
        collection.insertOne(new Document("firstName", "Anakin").append("lastName", "Skywalker").append("jedi", "true"));

        assertRowsAnyOrder("select firstName, lastName, jedi from " + methodName()
                        + " where lastName = ?",
                singletonList("Skywalker"),
                asList(
                        new Row("Luke", "Skywalker", true),
                        new Row("Anakin", "Skywalker", true)
                )
        );
    }

    @Test
    public void readsWithJoins() {
        final String connectionString = mongoContainer.getConnectionString();

        MongoCollection<Document> peopleName = database.getCollection("peopleName");
        peopleName.insertOne(new Document("personId", 1).append("name", "Luke Skywalker"));
        peopleName.insertOne(new Document("personId", 2).append("name", "Han Solo"));

        MongoCollection<Document> peopleProfession = database.getCollection("peopleProfession");
        peopleProfession.insertOne(new Document("personId", 1).append("profession", "Jedi"));
        peopleProfession.insertOne(new Document("personId", 2).append("profession", "Smuggler"));

        IMap<Integer, String> peopleBirthPlanet = instance().getMap("peopleBirthPlanet");
        peopleBirthPlanet.put(1, "Polis Massa");
        peopleBirthPlanet.put(2, "Corellia");

        execute("CREATE MAPPING peopleName external name \"peopleName\" (personId INT, name VARCHAR) "
                + "TYPE MongoDB "
                + "OPTIONS ("
                + "    'connectionString' = '" + connectionString + "', "
                + "    'database' = '" +  databaseName + "'"
                + ")");
        execute("CREATE MAPPING peopleProfession external name \"peopleProfession\" (personId INT, profession VARCHAR) "
                + "TYPE MongoDB "
                + "OPTIONS ("
                + "    'connectionString' = '" + connectionString + "', "
                + "    'database' = '" + databaseName + "'"
                + ")");
        execute("CREATE MAPPING peopleBirthPlanet (__key INT, this VARCHAR) "
                + "TYPE IMap "
                + "OPTIONS ("
                + "'keyFormat'='int',"
                + "'valueFormat'='java',"
                + "'valueJavaClass'='java.lang.String'"
                + ")");

        assertRowsAnyOrder("select pn.personId, pn.name, pr.profession, pl.this as birthPlanet " +
                        "from peopleName pn " +
                        "join peopleProfession pr on pn.personId = pr.personId " +
                        "join peopleBirthPlanet pl on pl.__key = pn.personId",
                emptyList(),
                asList(
                        new Row(1, "Luke Skywalker", "Jedi", "Polis Massa"),
                        new Row(2, "Han Solo", "Smuggler", "Corellia")
                )
        );
    }

    @Test
    public void insertsIntoMongo_parametrized_withId() {
        testInsertsIntoMongo(true, "insert into " + methodName() + "(jedi, firstName, lastName) values (?, 'Han', ?)",
                false, "Solo");
    }
    @Test
    public void insertsIntoMongo_hardcoded_withoutId() {
        testInsertsIntoMongo(false, "insert into " + methodName() + "(jedi, firstName, lastName) " +
                "values (false, 'Han', 'Solo')");
    }

    public void testInsertsIntoMongo(boolean includeId, String sql, Object... args) {
        final String collectionName = methodName();

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document("firstName", "temp").append("lastName", "temp").append("jedi", "true"));

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
    public void insertsIntoMongo_multiple() {
        MongoCollection<Document> collection = database.getCollection(methodName());
        collection.insertOne(new Document("firstName", "temp").append("lastName", "temp").append("jedi", true));

        createMapping(true);

        collection.deleteMany(Filters.empty());

        execute("insert into " + methodName() + "(firstName, lastName) " +
                "select 'Person ' || v, cast(v as varchar) " +
                "from table(generate_series(0,2))");

        ArrayList<Document> list = collection.find()
                                             .into(new ArrayList<>());
        assertThat(list)
                .extracting(d -> d.getString("firstName"))
                .containsExactlyInAnyOrder("Person 0", "Person 1", "Person 2");
    }

    @Test
    public void insertsIntoMongo_duplicate() {
        MongoCollection<Document> collection = database.getCollection(methodName());
        ObjectId insertedId =
                collection.insertOne(new Document("firstName", "temp").append("lastName", "temp").append("jedi",
                        true)).getInsertedId().asObjectId().getValue();
        createMapping(true);
        assertThatThrownBy(() -> execute("insert into " + methodName() + " (id, firstName, jedi) values (?, ?, ?)",
                insertedId, "yolo", false))
                .hasRootCauseInstanceOf(MongoBulkWriteException.class)
                .hasMessageContaining("E11000 duplicate key error collection");
    }

    @Test
    public void updatesMongo_allHardcoded() {
        testUpdatesMongo(true,
                "update " + methodName() + " set firstName = 'Han', lastName = 'Solo', jedi=false " +
                        "where jedi=true or firstName = 'Han'");
    }
    @Test
    public void updatesMongo_setParametrized() {
        testUpdatesMongo(true,
                "update " + methodName() + " set firstName = 'Han', lastName = ?, jedi=false " +
                        "where jedi=true or firstName = 'Han'", "Solo");
    }

    @Test
    public void updatesMongo_whereParametrized() {
        testUpdatesMongo(true,
                "update " + methodName() + " set firstName = 'Han', lastName = 'Solo', jedi=false " +
                        "where jedi=true or firstName = ?", "Han");
    }
    @Test
    public void updatesMongo_allParametrized() {
        testUpdatesMongo(true,
                "update " + methodName() + " set firstName = ?, lastName = ?, jedi=? " +
                        "where firstName = ?", "Han", "Solo", false, "temp");
    }

    public void testUpdatesMongo(boolean includeIdInMapping, String sql, Object... args) {
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
        testSinksIntoMongo(true, "sink into " + methodName() + " (firstName, lastName, jedi) values ('Leia', 'Organa', true)");
    }
    @Test
    public void sinkInto_allHardcoded_withoutId() {
        testSinksIntoMongo(false, "sink into " + methodName() + " (firstName, lastName, jedi) values ('Leia', 'Organa', true)");
    }

    @Test
    public void sinkInto_oneParametrized_withId() {
        testSinksIntoMongo(true, "sink into " + methodName() + " (firstName, lastName, jedi) values ('Leia', ?, true)",
                "Organa");
    }
    @Test
    public void sinkInto_oneParametrized_withoutId() {
        testSinksIntoMongo(false, "sink into " + methodName() + " (firstName, lastName, jedi) values ('Leia', ?, true)",
                "Organa");
    }

    public void testSinksIntoMongo(boolean includeId, String sql, Object... args) {
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
                + (includeIdInMapping ? " id OBJECT external name _id, " : "")
                + " firstName VARCHAR, "
                + " lastName VARCHAR, "
                + " jedi BOOLEAN "
                + ") "
                + "TYPE MongoDB "
                + "OPTIONS ("
                + "    'connectionString' = '" + mongoContainer.getConnectionString() + "', "
                + "    'database' = '" +  databaseName + "', "
                + "    'collection' = '" + methodName() + "' "
                + ")");
    }

}
