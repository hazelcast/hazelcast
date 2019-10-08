package com.hazelcast.internal.util;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class is threadsafe.
 */
public class CpuPool {

    private final List<Integer> cpus;
    private final Queue<Integer> pool = new ConcurrentLinkedQueue<>();
    private final List<Integer> usedCpus = new LinkedList<>();

    public CpuPool(String cpuString) {
        cpus = Collections.unmodifiableList(parseCpuString(cpuString));
        pool.addAll(cpus);
    }

    public List<Integer> usedCpus(){
        return usedCpus;
    }

    public void reset() {
        pool.clear();
        pool.addAll(cpus);
        usedCpus.clear();
    }

    public int take() {
        Integer cpu = pool.poll();
        if (cpu == null) {
            return -1;
        }
        usedCpus.add(cpu);
        return cpu;
    }

    public boolean isDisabled() {
        return cpus.isEmpty();
    }

    static List<Integer> parseCpuString(String cpuString) {
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
