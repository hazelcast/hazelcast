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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.EventTimePolicy.DEFAULT_IDLE_TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({QuickTest.class, ParallelJVMTest.class})
public class StreamSourceTest extends PipelineTestSupport {

    @Test
    public void test_defaultIdle() {
        test(DEFAULT_IDLE_TIMEOUT);
    }

    @Test
    public void test_shortIdle() {
        test(500);
    }

    private void test(long idleTimeout) {
        StreamSource<Object> source = SourceBuilder
                .stream("src", Context::globalProcessorIndex)
                .distributed(1)
                .fillBufferFn((index, buf) -> {
                    // Only 1 instance will generate output. Until other instances are marked
                    // idle, there should be no output.
                    if (index == 0) {
                        buf.add("item");
                        Thread.sleep(10);
                    }
                })
                .build();
        if (idleTimeout != DEFAULT_IDLE_TIMEOUT) {
            source = source.setPartitionIdleTimeout(idleTimeout);
        }

        Pipeline p = Pipeline.create();
        p.readFrom(source)
         .withTimestamps(o -> System.currentTimeMillis(), 0)
         .window(WindowDefinition.tumbling(100))
         .aggregate(counting())
         .writeTo(sink);

        Job job = allHazelcastInstances()[0].getJet().newJob(p);

        if (idleTimeout > 10_000) {
            assertTrueAllTheTime(() -> assertEquals("unexpected sink size", 0, sinkList.size()), 5);
        } else if (idleTimeout < 1000) {
            assertTrueEventually(() -> assertTrue("sink empty", sinkList.size() > 0));
        } else {
            fail("test not designed for idleTimeout=" + idleTimeout);
        }

        cancelAndJoin(job);
    }
}
