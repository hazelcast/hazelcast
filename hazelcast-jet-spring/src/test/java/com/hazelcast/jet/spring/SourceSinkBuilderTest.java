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

package com.hazelcast.jet.spring;

import com.hazelcast.collection.IQueue;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.spring.context.SpringAware;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;
import java.util.List;

import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertAnyOrder;
import static com.hazelcast.test.HazelcastTestSupport.assertContainsAll;
import static java.util.Arrays.asList;


@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"application-context-builder.xml"})
public class SourceSinkBuilderTest {

    @Resource(name = "jet-instance")
    private JetInstance jetInstance;

    @Resource(name = "my-queue-bean")
    private IQueue<Integer> queue;

    @Resource(name = "my-list-bean")
    private List<Integer> list;

    @BeforeClass
    @AfterClass
    public static void start() {
        Jet.shutdownAll();
    }

    @Test(timeout = 20000)
    public void testSpringAwareSinkContext() {
        Sink<Integer> sink = SinkBuilder.sinkBuilder("springaware-sink", c -> new SinkContext())
                .receiveFn(SinkContext::onReceive)
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1, 2, 3, 4, 5))
                .writeTo(sink);

        jetInstance.newJob(p).join();

        assertContainsAll(queue, asList(1, 2, 3, 4, 5));
    }

    @Test(timeout = 20000)
    public void testSpringAwareSourceContext() {
        list.addAll(asList(1, 2, 3, 4, 5));

        BatchSource<Integer> source = SourceBuilder.batch("springaware-source", c -> new SourceContext())
                .fillBufferFn(SourceContext::onFillBuffer)
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(source)
                .writeTo(assertAnyOrder(asList(1, 2, 3, 4, 5)));

        jetInstance.newJob(p).join();
    }

    @SpringAware
    public static final class SourceContext {
        @Resource(name = "my-list-bean")
        private List<Integer> list;

        public void onFillBuffer(SourceBuilder.SourceBuffer<Integer> buffer) {
            for (Integer i : list) {
                buffer.add(i);
            }
            buffer.close();
        }
    }


    @SpringAware
    public static final class SinkContext {
        @Resource(name = "my-queue-bean")
        private IQueue<Integer> queue;

        public void onReceive(int item) {
            queue.add(item);
        }
    }
}
