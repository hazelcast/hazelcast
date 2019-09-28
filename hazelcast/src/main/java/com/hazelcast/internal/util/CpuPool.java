package com.hazelcast.internal.util;

import net.openhft.affinity.Affinity;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class CpuPool {

    private List<Integer> cpus;
    private Queue<Integer> pool = new ConcurrentLinkedQueue<>();

    public CpuPool(String cpuString) {
        if (Affinity.isJNAAvailable()) {
            cpus = affinityCpuStringToList(cpuString);
            pool.addAll(cpus);
        } else {
            cpus = new LinkedList<>();
        }
    }

    public int deque() {
        Integer cpu = pool.poll();
        return cpu == null?-1:cpu;
    }

    public void enque(int cpu) {
        pool.add(cpu);
    }

    public static List<Integer> affinityCpuStringToList(String cpuString) {
        List<Integer> cpus = new ArrayList<>();
        if(cpuString == null){
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
