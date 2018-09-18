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

package com.hazelcast.internal.probing;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProbeThreadTest extends AbstractProbeTest {

    private static final ThreadMXBean MX_BEAN = ManagementFactory.getThreadMXBean();

    @Before
    public void setup() {
        registry.register(Probing.OS);
    }

    @Test
    public void threadCount() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertProbed("thread.threadCount", MX_BEAN.getThreadCount(), 10);
            }
        });
    }

    @Test
    public void peakThreadCount() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertProbed("thread.peakThreadCount", MX_BEAN.getPeakThreadCount(), 10);
            }
        });
    }

    @Test
    public void daemonThreadCount() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertProbed("thread.daemonThreadCount", MX_BEAN.getDaemonThreadCount(), 10);
            }
        });
    }

    @Test
    public void totalStartedThreadCount() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertProbed("thread.totalStartedThreadCount", MX_BEAN.getTotalStartedThreadCount(), 10);
            }
        });
    }
}
