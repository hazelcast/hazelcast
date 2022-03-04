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

package com.hazelcast.durableexecutor;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.ClusterProperty;
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
public class DurableLongRunningTaskTest extends HazelcastTestSupport {

    private static final int CALL_TIMEOUT = 3000;

    private HazelcastInstance hz;

    @Before
    public void setup() {
        Config config = smallInstanceConfig()
                .setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + CALL_TIMEOUT);
        hz = createHazelcastInstance(config);
    }

    @Test
    public void test() {
        final String response = "foobar";
        SleepingCallable task = new SleepingCallable(response, 6 * CALL_TIMEOUT);
        final Future<String> f = hz.getDurableExecutorService("e").submit(task);
        assertTrueEventually(() -> {
            assertTrue(f.isDone());
            assertEquals(response, f.get());
        });
    }

    public static class SleepingCallable implements Callable<String>, Serializable, HazelcastInstanceAware {

        private final String response;
        private final int delayMs;
        private ILogger logger;

        SleepingCallable(String response, int delayMs) {
            this.response = response;
            this.delayMs = delayMs;
        }

        @Override
        public String call() throws Exception {
            logger.info("SleepingCallable task started");
            Thread.sleep(delayMs);
            return response;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            logger = instance.getLoggingService().getLogger("DurableLongRunningTaskTest");
        }
    }
}
