/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
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
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertAnyOrder;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ManagedContextTest extends JetTestSupport {

    static final String INJECTED_VALUE = "injectedValue";
    private HazelcastInstance hz;

    @Before
    public void setup() {
        Config config = smallInstanceConfig().setManagedContext(new MockManagedContext());
        hz = createHazelcastInstance(config);
    }

    @Test
    public void when_managedContextSet_then_processorsInitWithContext() {
        testProcessors(TestProcessor::new);
    }

    @Test
    public void when_managedContextSet_then_differentProcessorReturnedFromContext() {
        testProcessors(AnotherTestProcessor::new);
    }

    private void testProcessors(SupplierEx<Processor> processorSupplier) {
        // Given
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.batchFromProcessor("testSource",
                ProcessorMetaSupplier.preferLocalParallelismOne(processorSupplier)))
         .writeTo(assertAnyOrder(singletonList(INJECTED_VALUE)));

        // When
        hz.getJet().newJob(p).join();
    }

    @Test
    public void when_managedContextSet_then_serviceContextInitialized() {
        testServices(pctx -> new TestServiceContext());
    }

    @Test
    public void when_managedContextSet_then_differentServiceContextReturnedFromContext() {
        testServices(pctx -> new AnotherTestServiceContext());
    }

    private void testServices(FunctionEx<ProcessorSupplier.Context, ? extends AnotherTestServiceContext> serviceSupplier) {
        // Given
        ServiceFactory<?, ? extends AnotherTestServiceContext> serviceFactory =
                ServiceFactories.sharedService(serviceSupplier);
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items("item"))
         .mapUsingService(serviceFactory, (c, item) -> item + c.injectedValue)
         .writeTo(assertAnyOrder(singletonList("item" + INJECTED_VALUE)));

        // When
        hz.getJet().newJob(p).join();
    }

    @Test
    public void when_managedContextSet_then_sourceContextInitializedWithContext() {
        testSources(SourceContext::new);
    }

    @Test
    public void when_managedContextSet_then_differentSourceContextReturnedFromContext() {
        testSources(AnotherSourceContext::new);
    }

    private void testSources(SupplierEx<? extends AnotherSourceContext> sourceSupplier) {
        BatchSource<String> src = SourceBuilder.batch("source", c -> sourceSupplier.get())
                .<String>fillBufferFn((c, b) -> {
                    b.add(c.injectedValue);
                    b.close();
                }).build();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(src)
                .writeTo(assertAnyOrder(singletonList(INJECTED_VALUE)));

        hz.getJet().newJob(pipeline).join();
    }

    @Test
    public void when_managedContextSet_then_SinkContextInitializedWithContext() {
        testSinks(SinkContext::new);
    }

    @Test
    public void when_managedContextSet_then_differentSinkContextReturnedFromContext() {
        testSinks(AnotherSinkContext::new);
    }

    private void testSinks(SupplierEx<? extends AnotherSinkContext> sinkSupplier) {
        Sink<Object> sink = SinkBuilder.sinkBuilder("sink", c -> sinkSupplier.get())
                                       .receiveFn((c, i) -> assertEquals(INJECTED_VALUE, c.injectedValue))
                                       .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(1))
                .writeTo(sink);

        hz.getJet().newJob(pipeline).join();
    }

    private static class MockManagedContext implements ManagedContext {

        @Override
        public Object initialize(Object obj) {
            if (obj instanceof AnotherTestProcessor) {
                return new TestProcessor().setInjectedValue(INJECTED_VALUE);
            }
            if (obj instanceof TestServiceContext) {
                ((TestServiceContext) obj).injectedValue = INJECTED_VALUE;
            } else if (obj instanceof AnotherTestServiceContext) {
                return new TestServiceContext().setInjectedValue(INJECTED_VALUE);
            }
            if (obj instanceof SourceContext) {
                ((SourceContext) obj).injectedValue = INJECTED_VALUE;
            } else if (obj instanceof AnotherSourceContext) {
                return new SourceContext().setInjectedValue(INJECTED_VALUE);
            }
            if (obj instanceof SinkContext) {
                ((SinkContext) obj).injectedValue = INJECTED_VALUE;
            } else if (obj instanceof AnotherSinkContext) {
                return new SinkContext().setInjectedValue(INJECTED_VALUE);
            }
            if (obj instanceof TestProcessor) {
                ((TestProcessor) obj).injectedValue = INJECTED_VALUE;
            }
            return obj;
        }
    }

    private static class TestServiceContext extends AnotherTestServiceContext {

        private TestServiceContext setInjectedValue(String injectedValue) {
            this.injectedValue = injectedValue;
            return this;
        }

    }

    private static class AnotherTestServiceContext {
        protected String injectedValue;
    }

    private static class TestProcessor extends AbstractProcessor {

        private String injectedValue;

        TestProcessor() {
        }

        public TestProcessor setInjectedValue(String injectedValue) {
            this.injectedValue = injectedValue;
            return this;
        }

        @Override
        public boolean complete() {
            return tryEmit(injectedValue);
        }
    }

    private static class AnotherTestProcessor extends AbstractProcessor {
    }

    private static final class SourceContext extends AnotherSourceContext {

        private SourceContext setInjectedValue(String injectedValue) {
            this.injectedValue = injectedValue;
            return this;
        }
    }

    private static class AnotherSourceContext {
        protected String injectedValue;
    }

    private static final class SinkContext extends AnotherSinkContext {

        private SinkContext setInjectedValue(String injectedValue) {
            this.injectedValue = injectedValue;
            return this;
        }
    }

    private static class AnotherSinkContext {
        protected String injectedValue;
    }

}
