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
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.mongodb.MongoSources.batch;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class})
public class MongoSourceVariousIdTypesTest extends AbstractMongoTest {

    @Parameter(0)
    public Object id1;
    @Parameter(1)
    public Object id2;
    @Parameter(2)
    public String idType;

    @Parameters(name = "IdType:{2}")
    public static Object[] filterProjectionSortMatrix() {
        return new Object[] {
                new Object[] { new ObjectId(), new ObjectId(), "ObjectId" },
                new Object[] { "a", "abc", "string" },
                new Object[] { 1, 2, "integer" },
                new Object[] { "a", 2, "string-integer" }
        };
    }

    @Test
    public void testIdType() {
        IList<Object> list = instance().getList(testName.getMethodName());
        final String connectionString = mongoContainer.getConnectionString();

        MongoDatabase database = mongo.getDatabase(defaultDatabase());
        MongoCollection<Document> collection = database.getCollection(testName.getMethodName());

        collection.insertOne(new Document("_id", id1));
        collection.insertOne(new Document("_id", id2));

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(batch(connectionString, defaultDatabase(), testName.getMethodName(), null, null))
                .setLocalParallelism(3)
                .writeTo(Sinks.list(list));

        instance().getJet().newJob(pipeline, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE)).join();

        assertEquals(2, list.size());
    }

}
