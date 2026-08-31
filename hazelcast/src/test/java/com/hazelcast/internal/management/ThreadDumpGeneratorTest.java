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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ThreadDumpGeneratorTest extends HazelcastTestSupport {

    @Test
    public void testDumpAllThreads_viaDiagnosticCommand() throws Exception {
        String expectedDump = "DiagnosticCommand thread dump output";
        MBeanServer mBeanServer = mock(MBeanServer.class);
        when(mBeanServer.invoke(any(), eq("threadPrint"), any(), any())).thenReturn(expectedDump);

        try (MockedStatic<ManagementFactory> managementFactory = mockStatic(ManagementFactory.class, CALLS_REAL_METHODS)) {
            managementFactory.when(ManagementFactory::getPlatformMBeanServer).thenReturn(mBeanServer);

            assertEquals(expectedDump, ThreadDumpGenerator.dumpAllThreads());
        }
    }

    @Test
    public void testDumpAllThreads_fallsBackWhenDiagnosticCommandFails() throws Exception {
        MBeanServer mBeanServer = mock(MBeanServer.class);
        when(mBeanServer.invoke(any(), any(), any(), any()))
                .thenThrow(new InstanceNotFoundException("DiagnosticCommand MBean not available"));

        try (MockedStatic<ManagementFactory> managementFactory = mockStatic(ManagementFactory.class, CALLS_REAL_METHODS)) {
            managementFactory.when(ManagementFactory::getPlatformMBeanServer).thenReturn(mBeanServer);

            String dump = ThreadDumpGenerator.dumpAllThreads();
            assertTrue("Expected ThreadMXBean fallback header", dump.startsWith("Full thread dump "));
            assertTrue("Expected current thread in fallback dump", dump.contains(Thread.currentThread().getName()));
        }
    }
}
