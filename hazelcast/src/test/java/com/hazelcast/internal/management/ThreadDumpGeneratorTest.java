/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ThreadDumpGeneratorTest extends HazelcastTestSupport {

    @Test
    public void testDumpAllThreads() {
        String dump = ThreadDumpGenerator.dumpAllThreads();
        // DiagnosticCommand produces native JVM thread dump format
        assertNotNull(dump);
        assertTrue("Expected 'Full thread dump' header in output", dump.contains("Full thread dump"));
        assertTrue("Expected current thread to appear in dump", dump.contains(Thread.currentThread().getName()));
    }

    @Test
    public void testDumpAllThreads_fallbackWhenDiagnosticCommandFails() throws Exception {
        MBeanServer mockMBeanServer = mock(MBeanServer.class);
        when(mockMBeanServer.invoke(any(), any(), any(), any()))
                .thenThrow(new InstanceNotFoundException("DiagnosticCommand MBean not available"));

        try (MockedStatic<ManagementFactory> mf = mockStatic(ManagementFactory.class, invocation -> {
            if (invocation.getMethod().getName().equals("getPlatformMBeanServer")) {
                return mockMBeanServer;
            }
            return invocation.callRealMethod();
        })) {
            String dump = ThreadDumpGenerator.dumpAllThreads();
            // Fallback via ThreadMXBean prepends "Full thread dump " and includes live thread info
            assertTrue("Expected 'Full thread dump' prefix from ThreadMXBean fallback", dump.startsWith("Full thread dump "));
            assertTrue("Expected current thread to appear in fallback dump", dump.contains(Thread.currentThread().getName()));
        }
    }

    @Test
    public void testDumpDeadlocks() {
        String dump = ThreadDumpGenerator.dumpDeadlocks();
        assertTrue("Expected 'Deadlocked thread dump' header in output", dump.contains("Deadlocked thread dump"));
    }

    @Test
    public void testFindDeadlockedThreads_noDeadlocks() {
        ThreadInfo[] result = ThreadDumpGenerator.findDeadlockedThreads();
        // No deadlocks in a normal test run
        assertTrue(result == null || result.length == 0);
    }

    @Test
    public void testGetAllThreads() {
        ThreadInfo[] threads = ThreadDumpGenerator.getAllThreads();
        assertNotNull(threads);
        assertTrue("Expected at least one thread", threads.length > 0);
        boolean currentThreadFound = false;
        for (ThreadInfo thread : threads) {
            if (Thread.currentThread().getName().equals(thread.getThreadName())) {
                currentThreadFound = true;
                break;
            }
        }
        assertTrue("Expected current thread to appear in getAllThreads()", currentThreadFound);
    }
}
