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

package com.hazelcast.executor;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LongRunningTaskTest extends HazelcastTestSupport {

    private static final int CALL_TIMEOUT = 1000;

    private HazelcastInstance hz;

    @Before
    public void setup() {
        Config config = new Config().setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + CALL_TIMEOUT);
        hz = createHazelcastInstance(config);
    }

    @Test
    public void test() {
        final String response = "foobar";
        SleepingCallable task = new SleepingCallable(response, 10 * CALL_TIMEOUT);
        final Future<String> future = hz.getExecutorService("e").submit(task);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(future.isDone());
                assertEquals(response, future.get());
            }
        });
    }

    public static class SleepingCallable implements Callable<String>, Serializable {

        private final String response;
        private final int delayMs;

        SleepingCallable(String response, int delayMs) {
            this.response = response;
            this.delayMs = delayMs;
        }

        @Override
        public String call() throws Exception {
            Thread.sleep(delayMs);
            return response;
        }
    }
}
