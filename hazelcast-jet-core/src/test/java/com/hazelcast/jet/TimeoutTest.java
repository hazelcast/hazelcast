/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.impl.AbstractProducer;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.TestUtil.executeAndPeel;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class TimeoutTest extends HazelcastTestSupport {

    private static final int TIMEOUT_MILLIS = 1000;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private TestHazelcastInstanceFactory factory;
    private Config config;

    @Before
    public void setup() {
        config = new Config();
        config.getProperties().put(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(),
                Integer.toString(TIMEOUT_MILLIS));
        factory = createHazelcastInstanceFactory();
    }

    @Test
    public void when_slowRunningOperationOnSingleNode_then_doesNotTimeout() throws Throwable {
        // Given
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        JetEngine jetEngine = JetEngine.get(instance, "jetEngine");
        DAG dag = new DAG();
        Vertex slow = new Vertex("slow", SlowProcessor::new);
        dag.addVertex(slow);

        // When
        executeAndPeel(jetEngine.newJob(dag));
    }

    @Test
    public void when_slowRunningOperationOnMultipleNodes_doesNotTimeout() throws Throwable {
        // Given
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        JetEngine jetEngine = JetEngine.get(instance, "jetEngine");

        DAG dag = new DAG();
        Vertex slow = new Vertex("slow", SlowProcessor::new);
        dag.addVertex(slow);

        // When
        executeAndPeel(jetEngine.newJob(dag));
    }

    private static class SlowProcessor extends AbstractProducer {

        @Override
        public boolean complete() {
            sleepMillis(5 * TIMEOUT_MILLIS);
            return true;
        }
    }

}
