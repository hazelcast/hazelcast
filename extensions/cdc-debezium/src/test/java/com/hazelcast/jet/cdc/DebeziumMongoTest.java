/*
 * Copyright 2026 Hazelcast Inc.
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
package com.hazelcast.jet.cdc;

import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ChangeStreamPreAndPostImagesOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.debezium.connector.mongodb.MongoDbConnector;
import org.bson.Document;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;

import javax.annotation.Nonnull;
import java.util.List;

import static com.hazelcast.jet.TestedVersions.MONGO_IMAGE;

@Category({NightlyTest.class})
@RunWith(HazelcastSerialClassRunner.class)
public class DebeziumMongoTest extends AbstractBasicCdcIntegrationTest<MongoDBContainer> {
    private static final Logger LOG = LoggerFactory.getLogger(DebeziumMongoTest.class);

    @Override
    public void prepareDatabase(MongoDBContainer container) {
        try (var client = getClient(container)) {
            MongoDatabase inventory = client.getDatabase("inventory");
            var options = new CreateCollectionOptions()
                    .changeStreamPreAndPostImagesOptions(new ChangeStreamPreAndPostImagesOptions(true));
            inventory.createCollection("customers", options);
            inventory.createCollection("products", options);
        }
        insertInitialData(container);
    }

    private void insertInitialData(MongoDBContainer container) {
        try (var client = getClient(container)) {
            MongoDatabase inventory = client.getDatabase("inventory");
            var customers = inventory.getCollection("customers");

            customers.insertMany(List.of(
                    new Document("id", 1001).append("first_name", "Sally")
                                            .append("last_name", "Thomas").append("email", "sally.thomas@acme.com"),
                    new Document("id", 1002).append("first_name", "George")
                                            .append("last_name", "Bailey").append("email", "gbailey@foobar.com"),
                    new Document("id", 1003).append("first_name", "Edward")
                                            .append("last_name", "Walker").append("email", "ed@walker.com"),
                    new Document("id", 1004).append("first_name", "Anne")
                                            .append("last_name", "Kretchmar").append("email", "annek@noanswer.org"))
            );
            var products = inventory.getCollection("products");
            products.insertOne(new Document("id", "1001").append("name", "scooter")
                                                         .append("description", "Small 2-wheel scooter")
                                                         .append("weight", 3.14f)
            );
        }
    }

    @Override
    @SuppressWarnings("ClassEscapesDefinedScope")
    protected FiltersMultipleTablesMetadata filtersMultipleTablesMetadata() {
        return new FiltersMultipleTablesMetadata("collection.include.list",
                changeRecord -> (String) changeRecord.source().toMap().get("collection"));
    }

    @Nonnull
    @Override
    public Pipeline getPipeline(StreamSource<ChangeRecord> source) {
        return TestUtils.getPipeline(source, changeRecord -> (String) changeRecord.source().toMap().get("collection"));
    }

    @Override
    protected void performSetOfChanges(MongoDBContainer container) {
        try (var client = getClient(container)) {
            var customers = client.getDatabase("inventory").getCollection("customers");
            customers.updateOne(Filters.eq("id", 1004), Updates.set("first_name", "Anne Marie" + testNumberModifier));
            customers.insertOne(new Document("id", 1005).append("first_name", "Jason" + testNumberModifier)
                                                        .append("last_name", "Bourne").append("email", "jason@bourne.org"));
            customers.deleteOne(Filters.eq("id", 1005));
        }
    }

    @Override
    protected void revertSetOfChanges(MongoDBContainer container) {
        try (var client = getClient(container)) {
            var customers = client.getDatabase("inventory").getCollection("customers");
            customers.updateOne(Filters.eq("id", 1004), Updates.set("first_name", "Anne"));
        }
    }

    @Override
    protected void performSecondSetOfChanges(MongoDBContainer container) {
        try (var client = getClient(container)) {
            var customers = client.getDatabase("inventory").getCollection("customers");
            customers.insertOne(new Document("id", 1007 + testNumberModifier).append("first_name", "Darth")
                                                        .append("last_name", "Vader").append("email", "vader@empire.com"));
        }
    }

    @Override
    protected void revertSecondSetOfChanges(MongoDBContainer container) {
        try (var client = getClient(container)) {
            var customers = client.getDatabase("inventory").getCollection("customers");
            customers.deleteOne(Filters.eq("id", 1007 + testNumberModifier));
        }
    }

    @Override
    protected void performChangeToProduct(MongoDBContainer container) {
        try (var client = getClient(container)) {
            var customers = client.getDatabase("inventory").getCollection("products");
            String value = "product_" + testNumberModifier;
            customers.insertOne(new Document("id", 100 + testNumberModifier).append("name", value)
                                                                            .append("description", value)
                                                                            .append("weight", 3.14f));
        }
        LOG.info("Performed a change to product");
    }

    @Override
    protected void revertChangeToProduct(MongoDBContainer container) {
        try (var client = getClient(container)) {
            var customers = client.getDatabase("inventory").getCollection("products");
            customers.deleteOne(Filters.eq("id", 100 + testNumberModifier));
        }
    }

    @Override
    @SuppressWarnings("resource")
    public @Nonnull MongoDBContainer getContainer() {
        return new MongoDBContainer(MONGO_IMAGE)
                .withSharding();
    }

    @Override
    protected @Nonnull DebeziumCdcSources.Builder<ChangeRecord> basicConf(MongoDBContainer container) {
        return DebeziumCdcSources
                .debezium("mongodb", MongoDbConnector.class)
                .setProperty("mongodb.connection.string", container.getReplicaSetUrl())
                .setProperty("tasks.max", "2")
                .setProperty("collection.include.list", "inventory.customers")
                .setProperty("database.include.list", "inventory")
                .setProperty("filters.match.mode", "literal")
                .setProperty("capture.mode", "change_streams_update_full_with_pre_image")

                .setProperty("notification.enabled.channels", "TestNotificationChannel")
                .setProperty("notification.TestNotificationChannel.uuid", uuidForNotifications)
                .setProperty("topic.prefix", "TESTS");
    }

    private static MongoClient getClient(MongoDBContainer container) {
        return MongoClients.create(container.getConnectionString());
    }
}
