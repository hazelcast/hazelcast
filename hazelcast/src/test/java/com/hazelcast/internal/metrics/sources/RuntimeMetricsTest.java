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

package com.hazelcast.internal.metrics.sources;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.internal.metrics.AbstractMetricsTest;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class RuntimeMetricsTest extends AbstractMetricsTest {

    private static final int TEN_MB = 10 * 1024 * 1024;

    private Runtime runtime;

    @Before
    public void setup() {
        register(new RuntimeMetrics());
        runtime = Runtime.getRuntime();
    }

    @Test
    public void freeMemory() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertCollected("ns=runtime freeMemory", runtime.freeMemory(), TEN_MB);
            }
        });
    }

    @Test
    public void totalMemory() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertCollected("ns=runtime totalMemory", runtime.totalMemory(), TEN_MB);
            }
        });
    }

    @Test
    public void maxMemory() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertCollected("ns=runtime maxMemory", runtime.maxMemory(), TEN_MB);
            }
        });
    }

    @Test
    public void usedMemory() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                long expected = runtime.totalMemory() - runtime.freeMemory();
                assertCollected("ns=runtime usedMemory", expected, TEN_MB);
            }
        });
    }

    @Test
    public void availableProcessors() {
        assertCollected("ns=runtime availableProcessors", runtime.availableProcessors());
    }

    @Test
    public void uptime() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                long expected = ManagementFactory.getRuntimeMXBean().getUptime();
                assertCollected("ns=runtime uptime", expected, TimeUnit.MINUTES.toMillis(1));
            }
        });
    }
}
