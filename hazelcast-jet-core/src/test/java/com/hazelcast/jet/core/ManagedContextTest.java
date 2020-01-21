/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertAnyOrder;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class ManagedContextTest extends JetTestSupport {

    static final String INJECTED_VALUE = "injectedValue";
    private JetInstance jet;

    @Before
    public void setup() {
        JetConfig jetConfig = new JetConfig()
                .configureHazelcast(hzConfig -> hzConfig.setManagedContext(new MockManagedContext()));
        jet = createJetMember(jetConfig);
    }

    @Test
    public void when_managedContextSet_then_processorsInitWithContext() {
        // Given
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.batchFromProcessor("testSource",
                ProcessorMetaSupplier.preferLocalParallelismOne(TestProcessor::new)))
         .writeTo(assertAnyOrder(singletonList(INJECTED_VALUE)));

        // When
        jet.newJob(p).join();
    }

    @Test
    public void when_managedContextSet_then_serviceContextInitialized() {
        // Given
        ServiceFactory<?, TestServiceContext> serviceFactory =
                ServiceFactories.sharedService(TestServiceContext::new, ConsumerEx.noop());
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items("item"))
         .mapUsingService(serviceFactory, (c, item) -> item + c.injectedValue)
         .writeTo(assertAnyOrder(singletonList("item" + INJECTED_VALUE)));

        // When
        jet.newJob(p).join();
    }

    @Test
    public void when_managedContextSet_then_sourceContextInitializedWithContext() {
        BatchSource<String> src = SourceBuilder.batch("source", c -> new SourceContext())
                .<String>fillBufferFn((c, b) -> {
                    b.add(c.injectedValue);
                    b.close();
                }).build();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(src)
                .writeTo(assertAnyOrder(singletonList(INJECTED_VALUE)));

        jet.newJob(pipeline).join();
    }

    @Test
    public void when_managedContextSet_then_SinkContextInitializedWithContext() {
        Sink<Object> sink = SinkBuilder.sinkBuilder("sink", c -> new SinkContext())
                                       .receiveFn((c, i) -> assertEquals(INJECTED_VALUE, c.injectedValue))
                                       .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(1))
                .writeTo(sink);

        jet.newJob(pipeline).join();
    }

    private static class MockManagedContext implements ManagedContext {

        @Override
        public Object initialize(Object obj) {
            if (obj instanceof TestProcessor) {
                ((TestProcessor) obj).injectedValue = INJECTED_VALUE;
            }
            if (obj instanceof TestServiceContext) {
                ((TestServiceContext) obj).injectedValue = INJECTED_VALUE;
            }
            if (obj instanceof SourceContext) {
                ((SourceContext) obj).injectedValue = INJECTED_VALUE;
            }
            if (obj instanceof SinkContext) {
                ((SinkContext) obj).injectedValue = INJECTED_VALUE;
            }
            return obj;
        }
    }

    private static class TestServiceContext {
        private String injectedValue;
    }

    private static class TestProcessor extends AbstractProcessor {

        private String injectedValue;

        @Override
        public boolean complete() {
            return tryEmit(injectedValue);
        }
    }

    private static final class SourceContext {
        private String injectedValue;
    }

    private static final class SinkContext {
        private String injectedValue;
    }

}
