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
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class MongoDBSourceResilencyTest extends AbstractMongoDBTest {

    @Test @Ignore
    public void testStream_whenServerDown() {
        shutdownNodeFactory();
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz = instanceFactory.newHazelcastInstance();
        HazelcastInstance serverToShutdown = instanceFactory.newHazelcastInstance();
        int itemCount = 40_000;

        Sink<Integer> setSink = SinkBuilder
                .sinkBuilder("set", c -> c.hazelcastInstance().getSet("set"))
                .<Integer>receiveFn(Set::add)
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(MongoDBSourceBuilder
                .stream(SOURCE_NAME, () -> mongoClient(mongoContainer.getConnectionString()))
                .database(defaultDatabase())
                .collection(testName.getMethodName(), Document.class)
                .mapFn(ChangeStreamDocument::getFullDocument)
                .build())
         .withNativeTimestamps(0)
                .setLocalParallelism(2)
         .map(doc -> doc.getInteger("key"))
         .writeTo(setSink);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                 .setSnapshotIntervalMillis(2000);
        Job job = hz.getJet().newJob(p, jobConfig);
        ISet<Integer> set = hz.getSet("set");

        spawn(() -> {
            for (int i = 0; i < itemCount; i++) {
                collection().insertOne(new Document("key", i));
            }
        });

        sleepSeconds(5);
        serverToShutdown.shutdown();

        assertTrueEventually(() ->
            assertEquals(itemCount, set.size())
        );

        job.cancel();
    }

    static MongoClient mongoClient(String connectionString) {
        MongoClientSettings settings = MongoClientSettings
                .builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .applyToClusterSettings(b -> {
                    b.serverSelectionTimeout(30, SECONDS);
                })
                .build();

        return MongoClients.create(settings);
    }

}
