/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertTrueEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminate;
import static org.junit.Assert.assertEquals;

public class TaskQueueTest {
    private Reactor reactor;

    @Before
    public void before() {
        reactor = Reactor.Builder.newReactorBuilder(ReactorType.NIO).build();
        reactor.start();
    }

    @After
    public void after() {
        terminate(reactor);
    }

    @Test
    public void test_metrics_taskErrorCount() {
        reactor.offer(new Runnable() {
            @Override
            public void run() {
                throw new RuntimeException();
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                TaskQueue.Metrics metrics = reactor.eventloop().defaultTaskQueue().metrics();
                assertEquals(1, metrics.taskErrorCount());
            }
        });
    }
}
