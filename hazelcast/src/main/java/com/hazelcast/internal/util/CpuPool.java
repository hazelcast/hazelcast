package com.hazelcast.internal.util;

import net.openhft.affinity.Affinity;
import net.openhft.affinity.AffinityLock;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class is threadsafe.
 */
public class CpuPool {

    public final static CpuPool EMPTY_POOL = new CpuPool(null);
    private final static boolean AFFINITY_AVAILABLE = isAffinityAvailable();

    private final List<Integer> cpus;
    private final Queue<Integer> pool = new ConcurrentLinkedQueue<>();

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
        if (true) {
            runPeterLawrey(r);
        } else {
            runPeterVeentjer(r);
        }
    }

    private void runPeterLawrey(Runnable r) {
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

    private void runPeterVeentjer(Runnable r) {
        Integer cpu = pool.poll();
        if (cpu == null) {
            r.run();
            return;
        }

        String threadId = getThreadId();
        System.out.println("threadId:" + threadId);
        taskSetLock(cpu, threadId);
        try {
            r.run();
        } finally {
            pool.add(cpu);
            //lock.release();
        }
    }

    private static void taskSetLock(Integer cpu, String threadId) {
        bash(threadId, "taskset -c " + cpu + " -p " + threadId);
    }

    private static void taskSetUnlock(String threadId) {
       // bash(threadId, "taskset -c " + cpu + " -p " + threadId);
    }

    private static void bash(String threadId, String command) {
        ProcessBuilder builder = new ProcessBuilder("/bin/bash", "-c", command);
        try {
            builder.redirectErrorStream(true);
            File log = new File("taskset" + threadId + ".log");
            builder.redirectOutput(log);
            Process p = builder.start();
            int exitCode = p.waitFor();
            System.out.println("exitCode:" + exitCode);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String getThreadId() {
        byte[] bo = new byte[100];
        String[] cmd = {"bash", "-c", "echo $PPID"};
        Process p = null;
        try {
            p = Runtime.getRuntime().exec(cmd);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            p.getInputStream().read(bo);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return new String(bo);
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
