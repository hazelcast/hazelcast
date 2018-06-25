/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.ManagedContext;
import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.core.Edge.between;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class ManagedContextTest extends JetTestSupport {

    static final String INJECTED_VALUE = "injectedValue";
    private JetInstance jet;

    @Before
    public void setup() {
        JetConfig jetConfig = new JetConfig();
        jetConfig.getHazelcastConfig().setManagedContext(new MockManagedContext());
        jet = this.createJetMember(jetConfig);
    }

    @Test
    public void when_managedContextSet_then_processorsInitWithContext() {
        // Given
        DAG dag = new DAG();
        Vertex p = dag.newVertex("p", TestProcessor::new).localParallelism(1);
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP("sink"));
        dag.edge(between(p, sink));

        // When
        jet.newJob(dag).join();

        // Then
        IListJet<Object> list = jet.getList("sink");
        assertEquals(singletonList(INJECTED_VALUE), list.subList(0, list.size()));
    }


    private static class MockManagedContext implements ManagedContext {

        @Override
        public Object initialize(Object obj) {
            if (obj instanceof TestProcessor) {
                ((TestProcessor) obj).injectedValue = INJECTED_VALUE;
            }
            return obj;
        }
    }

    private static class TestProcessor extends AbstractProcessor {

        private String injectedValue;

        @Override
        public boolean complete() {
            return tryEmit(injectedValue);
        }
    }
}
