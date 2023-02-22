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
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.WindowDefinition;
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
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class MongoDBSourcesWindowedTest extends AbstractMongoDBTest {

    @BeforeClass
    public static void beforeClass() {
        assumeDockerEnabled();
    }

    @Test
    public void testWindows() {
        StreamSource<? extends Document> streamSource = MongoDBSources.stream("src", mongoContainer.getConnectionString(),
                defaultDatabase(), testName.getMethodName(),
                null, null);

        IList<Object> result = instance().getList("result");

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(streamSource)
                .withIngestionTimestamps()
                .window(WindowDefinition.tumbling(100))
                .aggregate(counting())
                .writeTo(Sinks.list(result));

        MongoCollection<Document> collection =
                mongo.getDatabase(defaultDatabase()).getCollection(testName.getMethodName());
        AtomicInteger counter = new AtomicInteger(0);
        spawn(() -> {
            while (counter.get() < 100) {
                ObjectId key = ObjectId.get();
                collection.insertOne(new Document("test", "testowe").append("_id", key));
                counter.incrementAndGet();
            }
        });
        instance().getJet().newJob(pipeline);

        assertTrueEventually(() -> assertThat(result).isNotEmpty());
    }
}
