/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.impl.connector.AbstractProducer;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.TestUtil.executeAndPeel;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class TimeoutTest extends JetTestSupport {

    private static final int TIMEOUT_MILLIS = 8000;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JetTestInstanceFactory factory;
    private JetConfig config;

    @Before
    public void setup() {
        config = new JetConfig();
        config.getHazelcastConfig().getProperties().put(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(),
                Integer.toString(TIMEOUT_MILLIS));
        factory = new JetTestInstanceFactory();
    }

    @After
    public void shutdown() {
        factory.shutdownAll();
    }

    @Test
    public void when_slowRunningOperationOnSingleNode_then_doesNotTimeout() throws Throwable {
        // Given
        JetInstance instance = factory.newMember(config);
        DAG dag = new DAG();
        Vertex slow = new Vertex("slow", SlowProcessor::new);
        dag.addVertex(slow);

        // When
        executeAndPeel(instance.newJob(dag));
    }

    @Test
    public void when_slowRunningOperationOnMultipleNodes_doesNotTimeout() throws Throwable {
        // Given
        JetInstance instance = factory.newMember(config);
        factory.newMember(config);

        DAG dag = new DAG();
        Vertex slow = new Vertex("slow", SlowProcessor::new);
        dag.addVertex(slow);

        // When
        executeAndPeel(instance.newJob(dag));
    }

    private static class SlowProcessor extends AbstractProducer {

        @Override
        public boolean complete() {
            sleepMillis(2 * TIMEOUT_MILLIS);
            return true;
        }
    }

}
