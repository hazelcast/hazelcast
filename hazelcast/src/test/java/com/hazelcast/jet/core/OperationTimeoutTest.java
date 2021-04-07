/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import com.hazelcast.config.Config;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.core.TestUtil.executeAndPeel;

@RunWith(HazelcastParallelClassRunner.class)
public class OperationTimeoutTest extends JetTestSupport {

    private static final int TIMEOUT_MILLIS = 8000;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private Config config;

    @Before
    public void setup() {
        config = new Config();
        config.setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), Integer.toString(TIMEOUT_MILLIS));
    }

    @Test
    public void when_slowRunningOperationOnSingleNode_then_doesNotTimeout() throws Throwable {
        // Given
        JetInstance instance = createJetMember(config);
        DAG dag = new DAG();
        dag.newVertex("slow", SlowProcessor::new);

        // When
        executeAndPeel(instance.newJob(dag));
    }

    @Test
    public void when_slowRunningOperationOnMultipleNodes_doesNotTimeout() throws Throwable {
        // Given
        JetInstance instance = createJetMember(config);
        createJetMember(config);

        DAG dag = new DAG();
        dag.newVertex("slow", SlowProcessor::new);

        // When
        executeAndPeel(instance.newJob(dag));
    }

    private static class SlowProcessor extends AbstractProcessor {

        @Override
        public boolean complete() {
            sleepMillis(2 * TIMEOUT_MILLIS);
            return true;
        }
    }

}
