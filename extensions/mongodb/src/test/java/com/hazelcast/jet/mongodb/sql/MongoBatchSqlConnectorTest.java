package com.hazelcast.jet.mongodb.sql;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.EmptyStatement;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.logging.LogListener;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MongoDBContainer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class MongoBatchSqlConnectorTest extends SqlTestSupport {
    static final String TEST_MONGO_VERSION = System.getProperty("test.mongo.version", "6.0.3");

    @ClassRule
    public static final MongoDBContainer mongoContainer
            = new MongoDBContainer("mongo:" + TEST_MONGO_VERSION);

    @Rule
    public final TestName testName = new TestName();

    private static SqlService sqlService;



    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
        sqlService = instance().getSql();
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
        final String databaseName = "sqlConnectorTest";
        final String collectionName = testName.getMethodName();
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

        MongoClient mongoClient = MongoClients.create(connectionString);
        MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(collectionName);
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
                + "    'database' = '" + databaseName + "', "
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

}