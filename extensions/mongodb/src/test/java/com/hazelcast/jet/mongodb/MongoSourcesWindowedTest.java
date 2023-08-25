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

package com.hazelcast.jet.mongodb;

import com.hazelcast.collection.IList;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.pipeline.Sinks.list;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class MongoSourcesWindowedTest extends AbstractMongoTest {

    @BeforeClass
    public static void beforeClass() {
        assumeDockerEnabled();
    }

    @Test
    public void testWindows() {
        StreamSource<? extends Document> streamSource = MongoSources
                .stream(mongoContainer.getConnectionString(), defaultDatabase(), testName.getMethodName(),
                        null, null)
                .setPartitionIdleTimeout(1000);

        IList<WindowResult<Long>> result = instance().getList("result");

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(streamSource)
                .withNativeTimestamps(0).setLocalParallelism(2)
                .peek()
                .groupingKey(d -> d.getString("test"))
                .window(tumbling(5))
                .aggregate(counting())
                .peek()
                .writeTo(list(result));

        MongoCollection<Document> collection =
                mongo.getDatabase(defaultDatabase()).getCollection(testName.getMethodName());
        AtomicInteger counter = new AtomicInteger(0);
        spawn(() -> {
            while (counter.get() < 20) {
                ObjectId key = ObjectId.get();
                collection.insertOne(new Document("test", "testowe").append("_id", key));
                counter.incrementAndGet();
            }
            for (int i = 0; i < 5; i++) {
                ObjectId key = ObjectId.get();
                collection.insertOne(new Document("test", "other").append("_id", key));
            }
        });
        Job job = instance().getJet().newJob(pipeline);
        assertJobStatusEventually(job, RUNNING);

        assertTrueEventually(() -> {
            assertThat(result).isNotEmpty();
            long currentSum = result
                    .stream()
                    .filter(w -> ((KeyedWindowResult<String, Long>) w).getKey().equalsIgnoreCase("testowe"))
                    .mapToLong(WindowResult::result)
                    .sum();
            assertThat(currentSum)
                    .as("Sum of windows is equal to input element count")
                    .isEqualTo(counter.get());
        }, 20);
    }
}
