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
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class MongoStreamSqlConnectorTest extends MongoSqlTest  {
    private final Random random = new Random();

    @Test
    public void readsFromMongo_withId_withUnsupportedExpr() {
       testReadsFromMongo(true, true);
    }

    @Test
    public void readsFromMongo_withId_withoutUnsupportedExpr() {
       testReadsFromMongo(true, false);
    }

    @Test
    public void readsFromMongo_withoutId_withUnsupportedExpr() {
       testReadsFromMongo(false, true);
    }

    @Test
    public void readsFromMongo_withoutId_withoutUnsupportedExpr() {
       testReadsFromMongo(false, false);
    }

    public void testReadsFromMongo(boolean includeIdInMapping, boolean useUnsupportedExpr) {
        final String databaseName = "sqlConnectorTest";
        final String collectionName = testName.getMethodName();
        final String tableName = testName.getMethodName();
        final String connectionString = mongoContainer.getConnectionString();

        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(collectionName);
            collection.insertOne(new Document("firstName", "temp").append("lastName", "temp").append("jedi", true));

            long startAt = System.currentTimeMillis();
            execute("CREATE MAPPING " + tableName + " external name "  + databaseName + "." + collectionName
                    + " ("
                    + (includeIdInMapping ? " id VARCHAR external name \"fullDocument._id\", " : "")
                    + " firstName VARCHAR, "
                    + " lastName VARCHAR, "
                    + " jedi BOOLEAN, "
                    + " operation VARCHAR external name operationType"
                    + ") "
                    + "TYPE MongoStream "
                    + "OPTIONS ("
                    + "    'connectionString' = '" + connectionString + "', "
                    + "    'startAt' = '" + startAt + "' "
                    + ")");

            String force = useUnsupportedExpr ? " and cast(jedi as varchar) = 'true' " : "";
            spawn(() -> {
                sleep(1000);
                collection.insertOne(new Document("firstName", "Luke").append("lastName", "Skywalker").append("jedi", true));
                sleep(100);
                collection.insertOne(new Document("firstName", "Han").append("lastName", "Solo").append("jedi", false));
                sleep(100);
                collection.insertOne(new Document("firstName", "Anakin").append("lastName", "Skywalker").append("jedi", true));
            });
            assertRowsEventuallyInAnyOrder("select firstName, lastName, operation from " + tableName
                            + " where lastName = ? and jedi=true" + force,
                    singletonList("Skywalker"),
                    asList(
                            new Row("Luke", "Skywalker", "insert"),
                            new Row("Anakin", "Skywalker", "insert")
                    ),
                    TimeUnit.SECONDS.toMillis(30)
            );
        }
    }

    @Test
    public void readsStreamWithTimestamps() {
        final String collectionName = randomName();
        final String tableName = collectionName;
        final String connectionString = mongoContainer.getConnectionString();

        MongoClient mongoClient = MongoClients.create(connectionString);
        MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(collectionName);
        collection.insertOne(new Document("firstName", "temp").append("lastName", "temp").append("jedi", true));

        long startAt = System.currentTimeMillis();
        execute("CREATE MAPPING " + tableName + " external name " + databaseName + "." + collectionName
                + " ("
                + " id VARCHAR external name \"fullDocument._id\", "
                + " firstName VARCHAR, "
                + " lastName VARCHAR, "
                + " jedi BOOLEAN, "
                + " operation VARCHAR external name operationType, "
                + " ts timestamp"
                + ") "
                + "TYPE MongoStream "
                + "OPTIONS ("
                + "    'connectionString' = '" + connectionString + "', "
                + "    'database' = '" + databaseName + "', "
                + "    'startAt' = '" + startAt + "' "
                + ")");

        spawn(() -> {
            assertTrueEventually(() -> assertEquals(1, instance().getJet().getJobs().size()));
            sleep(200);
            collection.insertOne(new Document("firstName", "Luke").append("lastName", "Skywalker").append("jedi", true));
            sleep(200);
            collection.insertOne(new Document("firstName", "Han").append("lastName", "Solo").append("jedi", false));
            sleep(100);
            collection.insertOne(new Document("firstName", "Anakin").append("lastName", "Skywalker").append("jedi", true));
        });
        assertRowsEventuallyInAnyOrder("select firstName, lastName, operation from table(impose_order("
                        + "    table " + tableName + ", "
                        + "    descriptor(ts), "
                        + "    interval '0.002' second "
                        + "))"
                        + "where lastName = 'Skywalker' limit 2",
                emptyList(),
                asList(
                        new Row("Luke", "Skywalker", "insert"),
                        new Row("Anakin", "Skywalker", "insert")
                )
        );
    }

    private void sleep(int howMuch) {
        try {
            Thread.sleep(random.nextInt(howMuch));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
