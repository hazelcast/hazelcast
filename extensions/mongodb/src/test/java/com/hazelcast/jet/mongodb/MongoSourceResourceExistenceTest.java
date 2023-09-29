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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.client.MongoClients;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static com.hazelcast.jet.mongodb.MongoSources.batch;
import static com.hazelcast.jet.mongodb.ResourceChecks.NEVER;
import static com.hazelcast.jet.mongodb.ResourceChecks.ONCE_PER_JOB;
import static com.hazelcast.jet.mongodb.ResourceChecks.ON_EACH_CONNECT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class})
public class MongoSourceResourceExistenceTest extends AbstractMongoTest {

    @Parameter(0)
    public boolean dbExists;
    @Parameter(1)
    public boolean collectionExists;
    @Parameter(2)
    public ResourceChecks checkRequested;
    @Parameter(3)
    public boolean shouldFail;

    @Parameters(name = "dbExists: {0} | collectionExists: {1} | checkRequested: {2} | shouldFail:{3}")
    public static Object[] filterProjectionSortMatrix() {
        return new Object[][] {
                new Object[] { true, true, ONCE_PER_JOB, false },
                new Object[] { true, true, ON_EACH_CONNECT, false },
                new Object[] { true, true, NEVER, false },
                new Object[] { false, true, NEVER, false },
                new Object[] { true, false, NEVER, false },
                new Object[] { false, true, ONCE_PER_JOB, true },
                new Object[] { false, true, ON_EACH_CONNECT, true },
                new Object[] { true, false, ONCE_PER_JOB, true },
                new Object[] { true, false, ON_EACH_CONNECT, true },
                new Object[] { false, false, ONCE_PER_JOB, true },
                new Object[] { false, false, ON_EACH_CONNECT, true },
        };
    }

    @Test
    public void test_errors_dbNotExistAndCheckRequested() {
        final String connectionString = mongoContainer.getConnectionString();
        Pipeline pipeline = Pipeline.create();
        String dbName = dbExists ? defaultDatabase() : "nonExisting";
        String colName = collectionExists ? testName.getMethodName() : "nonExisting";

        pipeline.readFrom(batch(() -> MongoClients.create(connectionString))
                        .database(dbName)
                        .collection(colName)
                        .checkResourceExistence(checkRequested)
                        .build()
                )
                .setLocalParallelism(2)
                .writeTo(Sinks.logger());

        if (shouldFail) {
            assertThatThrownBy(() -> instance().getJet().newJob(pipeline).join())
                    .hasCauseInstanceOf(JetException.class)
                    .hasMessageContaining("does not exist");
        } else {
            instance().getJet().newJob(pipeline).join();
        }
    }

}
