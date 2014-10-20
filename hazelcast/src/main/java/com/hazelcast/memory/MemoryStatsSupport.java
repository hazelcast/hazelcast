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
