/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.mongodb;

import com.hazelcast.collection.ISet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Set;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class MongoDBSourceResilienceTest extends SimpleTestInClusterSupport {
    private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("mongo:6.0.3");
    @ClassRule
    public static MongoDBContainer mongoContainer = new MongoDBContainer(DOCKER_IMAGE_NAME);

    @Test
    public void testStream_whenServerDown() {
        HazelcastInstance hz = createHazelcastInstance();
        HazelcastInstance serverToShutdown = createHazelcastInstance();
        JobRepository jobRepository = new JobRepository(hz);
        int itemCount = 10_000;

        Sink<Integer> setSink = SinkBuilder
                .sinkBuilder("set", c -> c.hazelcastInstance().getSet("set"))
                .<Integer>receiveFn(Set::add)
                .build();

        Pipeline pipeline = Pipeline.create();
        String databaseName = "shutdownTest";
        String collectionName = "test";
        pipeline.readFrom(MongoDBSourceBuilder
                        .stream("mongo source", () -> mongoClient(mongoContainer.getConnectionString()))
                        .database(databaseName)
                        .collection(collectionName, Document.class)
                        .mapFn(ChangeStreamDocument::getFullDocument)
                        .startAtOperationTime(new BsonTimestamp(System.currentTimeMillis()))
                        .build())
                .withNativeTimestamps(0)
                .setLocalParallelism(4)
                .map(doc -> doc.getInteger("key"))
                .writeTo(setSink);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(EXACTLY_ONCE)
                 .setSnapshotIntervalMillis(1000);
        Job job = hz.getJet().newJob(pipeline, jobConfig);
        ISet<Integer> set = hz.getSet("set");
        assertJobStatusEventually(job, RUNNING);

        spawn(() -> {
            try (MongoClient mongoClient = MongoClients.create(mongoContainer.getConnectionString())) {
                MongoCollection<Document> collection =
                        mongoClient.getDatabase(databaseName).getCollection(collectionName);

                for (int i = 0; i < itemCount/2; i++) {
                    collection.insertOne(new Document("key", i));
                }
            }
        });
        waitForFirstSnapshot(jobRepository, job.getId(), 30, false);
        assertTrueEventually(() -> assertGreaterOrEquals("should have some records", set.size(), 1));

        spawn(() -> {
            try (MongoClient mongoClient = MongoClients.create(mongoContainer.getConnectionString())) {
                MongoCollection<Document> collection =
                        mongoClient.getDatabase(databaseName).getCollection(collectionName);

                for (int i = itemCount/2; i < itemCount; i++) {
                    collection.insertOne(new Document("key", i));
                }
            }
        });

        serverToShutdown.shutdown();

        assertTrueEventually(() ->
            assertEquals(itemCount, set.size())
        );
    }

    static MongoClient mongoClient(String connectionString) {
        MongoClientSettings settings = MongoClientSettings
                .builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(30, SECONDS))
                .build();

        return MongoClients.create(settings);
    }

}
