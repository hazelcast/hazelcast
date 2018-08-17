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

package com.hazelcast.jet.spring;

import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.spring.CustomSpringJUnit4ClassRunner;
import com.hazelcast.spring.context.SpringAware;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Nonnull;
import javax.annotation.Resource;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"application-context-processor.xml"})
public class TestCustomProcessor {

    @Resource(name = "jet-instance")
    private JetInstance jetInstance;

    @BeforeClass
    @AfterClass
    public static void start() {
        Jet.shutdownAll();
    }

    @Test(timeout = 20000)
    public void test() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.batchFromProcessor("source", preferLocalParallelismOne(CustomSourceP::new)))
         .drainTo(Sinks.fromProcessor("sink", preferLocalParallelismOne(CustomSinkP::new)));

        jetInstance.newJob(p).join();
    }

    @SpringAware
    private static class CustomSourceP extends AbstractProcessor {

        @Resource(name = "my-list-bean")
        private IListJet list;

        @Override
        protected void init(@Nonnull Context context) {
            assertNotNull(list);
        }

        @Override
        public boolean complete() {
            return tryEmit("singleItem");
        }
    }

    @SpringAware
    private static class CustomSinkP extends AbstractProcessor {

        @Resource(name = "my-map-bean")
        private IMapJet map;

        @Override
        protected void init(@Nonnull Context context) {
            assertNotNull(map);
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            assertEquals("singleItem", item);
            return true;
        }
    }

}
