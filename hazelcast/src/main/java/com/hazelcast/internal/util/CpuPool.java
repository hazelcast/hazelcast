package com.hazelcast.internal.util;

import net.openhft.affinity.Affinity;
import net.openhft.affinity.AffinityLock;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class is threadsafe.
 */
public class CpuPool {
    public final static CpuPool EMPTY_POOL = new CpuPool(null);

    private final List<Integer> cpus;
    private final Queue<Integer> pool = new ConcurrentLinkedQueue<>();
    private final List<Integer> usedCpus = new LinkedList<>();

    public CpuPool(String cpuString) {
        cpus = Collections.unmodifiableList(parseCpuString(cpuString));
        pool.addAll(cpus);
        System.out.println("IsAffinityAvailable:"+isAffinityAvailable());
    }

    public List<Integer> availableCpus(){return cpus;}

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

    private static boolean isAffinityAvailable() {
        try {
            boolean jnaAvailable = Affinity.isJNAAvailable();
            if (!jnaAvailable) {
                System.err.println("jna is not available");
            }
            return jnaAvailable;
        } catch (NoClassDefFoundError e) {
            e.printStackTrace();
            System.err.println("Affinity jar isn't available");
            return false;
        }
        //return true;
    }

    public void run(Runnable r) {
        Integer cpu = pool.poll();
        if (cpu == null) {
            r.run();
            System.err.println("Failed to allocate a cpu");
            return;
        }

        AffinityLock lock = AffinityLock.acquireLock(cpu);
        System.out.println("Running with affinity");
        try {
            r.run();
        } finally {
            pool.add(cpu);
            lock.release();
        }
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

//
//public class CpuPool {
//
//    private final static boolean AFFINITY_AVAILABLE = isAffinityAvailable();
//
//    private final List<Integer> cpus;
//    private final Queue<Integer> pool = new ConcurrentLinkedQueue<>();
//
//    public CpuPool(String cpuString) {
//        if (AFFINITY_AVAILABLE) {
//            cpus = parseCpuString(cpuString);
//            pool.addAll(cpus);
//        } else {
//            cpus = new LinkedList<>();
//        }
//    }
//
//    public void reset(){
//        pool.clear();
//        pool.addAll(cpus);
//    }
//
//    public int take() {
//        Integer cpu = pool.poll();
//        if (cpu == null) {
//            return -1;
//        }
//
//        return cpu;
//    }
//
//    public boolean isDisabled() {
//        return cpus.isEmpty();
//    }
//
//    private static boolean isAffinityAvailable() {
//        try {
//            boolean jnaAvailable = Affinity.isJNAAvailable();
//            if (!jnaAvailable) {
//                System.err.println("jna is not available");
//            }
//            return jnaAvailable;
//        } catch (NoClassDefFoundError e) {
//            e.printStackTrace();
//            System.err.println("Affinity jar isn't available");
//            return false;
//        }
//        //return true;
//    }
//
//    public void run(Runnable r) {
//        Integer cpu = pool.poll();
//        if (cpu == null) {
//            r.run();
//            System.err.println("Failed to allocate a cpu");
//            return;
//        }
//
//        AffinityLock lock = AffinityLock.acquireLock(cpu);
//        System.out.println("Running with affinity");
//        try {
//            r.run();
//        } finally {
//            pool.add(cpu);
//            lock.release();
//        }
//    }
//
//}
//
