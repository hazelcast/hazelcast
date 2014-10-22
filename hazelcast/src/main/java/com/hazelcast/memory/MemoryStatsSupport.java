/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.memory;

import com.hazelcast.monitor.LocalInstanceStats;
import com.hazelcast.monitor.LocalMemoryStats;
import com.hazelcast.monitor.impl.LocalMemoryStatsImpl;
import com.hazelcast.util.EmptyStatement;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;

/**
 * This class provides heap usage statistics
 *
 */
public final class MemoryStatsSupport {

    /**
     * No public constructor is needed for utility classes
     */
    private MemoryStatsSupport() { }

    public static long totalPhysicalMemory() {
        return queryPhysicalMemory("TotalPhysicalMemorySize");
    }

    public static long freePhysicalMemory() {
        return queryPhysicalMemory("FreePhysicalMemorySize");
    }

    private static long queryPhysicalMemory(String type) {
        try {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName("java.lang", "type", "OperatingSystem");
            Object attribute = mBeanServer.getAttribute(name, type);
            if (attribute != null) {
                return Long.parseLong(attribute.toString());
            }
        } catch (Exception ignored) {
            EmptyStatement.ignore(ignored);
        }
        return -1L;
    }

    public static MemoryUsage getHeapMemoryUsage() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    }

    public static LocalMemoryStats getMemoryStats() {
        LocalMemoryStatsImpl stats = new LocalMemoryStatsImpl();
        stats.setTotalPhysical(totalPhysicalMemory());
        stats.setFreePhysical(freePhysicalMemory());

        MemoryUsage memoryUsage = getHeapMemoryUsage();
        stats.setMaxHeap(memoryUsage.getMax());
        stats.setCommittedHeap(memoryUsage.getCommitted());
        stats.setUsedHeap(memoryUsage.getUsed());

        stats.setCommittedNativeMemory(LocalInstanceStats.STAT_NOT_AVAILABLE);
        stats.setMaxNativeMemory(LocalInstanceStats.STAT_NOT_AVAILABLE);
        stats.setUsedNativeMemory(LocalInstanceStats.STAT_NOT_AVAILABLE);
        stats.setFreeNativeMemory(LocalInstanceStats.STAT_NOT_AVAILABLE);

        stats.setGcStats(GCStatsSupport.getGCStats());

        return stats;
    }
}
