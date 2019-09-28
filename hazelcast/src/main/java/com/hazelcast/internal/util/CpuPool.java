package com.hazelcast.internal.util;

import net.openhft.affinity.Affinity;
import net.openhft.affinity.AffinityLock;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class CpuPool {

    public final static CpuPool EMPTY_POOL = new CpuPool(null);
    private final static boolean AFFINITY_AVAILABLE = isAffinityAvailable();

    private List<Integer> cpus;
    private Queue<Integer> pool = new ConcurrentLinkedQueue<>();

    public CpuPool(String cpuString) {
        if (AFFINITY_AVAILABLE) {
            cpus = parseCpuString(cpuString);
            pool.addAll(cpus);
        } else {
            cpus = new LinkedList<>();
        }
    }

    public boolean isDisabled() {
        return cpus.isEmpty();
    }

    private static boolean isAffinityAvailable() {
        try {
            return Affinity.isJNAAvailable();
        } catch (NoClassDefFoundError e) {
            return false;
        }
    }

    public void runWithAffinityLock(Runnable r) {
        Integer cpu = pool.poll();
        if (cpu == null) {
            r.run();
            return;
        }

        AffinityLock lock = AffinityLock.acquireLock(cpu);

        try {
            r.run();
        } finally {
            pool.add(cpu);
            lock.release();
        }
    }

    public static List<Integer> parseCpuString(String cpuString) {
        List<Integer> cpus = new ArrayList<>();
        if (cpuString == null) {
            return cpus;
        }

        cpuString = cpuString.trim();
        if (cpuString.isEmpty()) {
            return cpus;
        }

        for (String s : cpuString.split(",")) {
            int indexOf = s.indexOf("-");
            if (indexOf >= 0) {
                int from = Integer.parseInt(s.substring(0, indexOf));
                int to = Integer.parseInt(s.substring(indexOf + 1));
                for (int cpu = from; cpu <= to; cpu++) {
                    if (!cpus.contains(cpu)) {
                        cpus.add(cpu);
                    }
                }
            } else {
                int cpu = Integer.parseInt(s);
                if (!cpus.contains(cpu)) {
                    cpus.add(cpu);
                }
            }
        }
        return cpus;
    }
}
