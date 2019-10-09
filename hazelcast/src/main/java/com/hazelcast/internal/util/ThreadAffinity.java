package com.hazelcast.internal.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static java.lang.Float.parseFloat;

public class ThreadAffinity {

    private static volatile int pid = 0;
    private static volatile String javaDir = null;
    private final static ConcurrentMap<Long, Integer> tids = new ConcurrentReferenceHashMap<>();

    public static void main(String[] args) throws InterruptedException {
        Thread thread = Thread.currentThread();

        System.out.println("pid:" + getPid());
        System.out.println("tid:" + getTid(thread));
        System.out.println("original thread affinity:" + getThreadAffinityBitmask(thread));

        System.out.println("Changing thread affinity");
        setThreadAffinity(Thread.currentThread(), 5);
        System.out.println("Changing affinity:" + getThreadAffinityBitmask(thread));

        System.out.println("Resetting thread affinity");
        resetThreadAffinity(Thread.currentThread());
        System.out.println("Reset affinity:" + getThreadAffinityBitmask(thread));

        //   System.out.println("tid from affinity:"+Affinity.getThreadId());
        Thread.sleep(1000000);
    }

    public static String affinityReport() {
        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        int cpus = Runtime.getRuntime().availableProcessors();

        Map<Thread, Long> threadAffinityMap = new HashMap<>();
        for (Thread thread : threadSet) {
            threadAffinityMap.put(thread, getThreadAffinityBitmask(thread));
        }

        StringBuffer sb = new StringBuffer();
        for (int cpu = 0; cpu < cpus; cpu++) {

        }

        return sb.toString();
    }

    public static String javaDirectory() {
        if (javaDir != null) {
            return javaDir;
        }

        String javaHome = System.getProperty("java.home");
        javaDir = javaHome.substring(0, javaHome.length() - 3);
        return javaDir;
    }

    public static synchronized void setThreadAffinity(Thread t, int cpu) {
        if (cpu == -1) {
            return;
        }

        System.out.println("----------------------------------------------------");
        int tid = getTid(t);
        System.out.println("Thread:" + t.getName());
        System.out.println("Tid:" + tid);
        System.out.println("Cpu:" + cpu);
        //  long bitmask = 1L << cpu;
        //  System.out.println("bitmark(decimal):"+bitmask);
        //  String bitmaskString = "0x" + Long.toHexString(bitmask);
        String command = "taskset -cp " + cpu + " " + tid;
        System.out.println("setCpusAllowed:" + command);
        String output = Bash.bash(command);
        System.out.println("[" + output + "]");
        System.out.println("----------------------------------------------------");
    }

    public static long getThreadAffinityBitmask(Thread t) {
        int tid = getTid(t);
        String command = "taskset -p " + tid;
        String[] results = Bash.bash(command).split(":");
        String result = results[results.length - 1].trim();
        //System.out.println("result:" + result);
        return Integer.parseInt(result, 16);
    }

    public static void setPriority(Thread t, int priority) {
        throw new RuntimeException("Unsupported operation exception");
    }

    //todo: how to deal with more than 64 cores??
    public static void resetThreadAffinity(Thread t) {
        setCpusAllowed(t, "0xFFFFFFFF");
    }

    public static synchronized void setCpusAllowed(Thread t, String bitmask) {
        System.out.println("----------------------------------------------------");
        int tid = getTid(t);
        System.out.println("Thread:" + t.getName());
        System.out.println("Tid:" + tid);
        String command = "taskset -p " + bitmask + " " + tid;
        System.out.println("setCpusAllowed:" + command);
        String output = Bash.bash(command);
        System.out.println("[" + output + "]");
        System.out.println("----------------------------------------------------");

        //  System.out.println(command);
    }

    public static int getTid(Thread t) {
        Integer tid = tids.get(t.getId());
        if (tid != null) {
            return tid;
        }

        String result = Bash.bash(javaDirectory() + "bin/jcmd " + getPid() + " Thread.print");
        String[] lines = result.split("\n");
        for (String line : lines) {
            //todo: we should cache all.
            if (line.startsWith("\"" + t.getName() + "\"")) {
                int indexOf = line.indexOf("nid=0x") + 5;
                int end = line.indexOf(" ", indexOf);
//                System.out.println(line);
//                System.out.println();
                String substring = line.substring(indexOf + 1, end);
//                System.out.println(substring);
                return Integer.parseInt(substring, 16);
            }
        }
        return -1;
    }

    public static int getPid() {
        if (pid != 0) {
            return pid;
        }

        try {
            java.lang.management.RuntimeMXBean runtime =
                    java.lang.management.ManagementFactory.getRuntimeMXBean();
            java.lang.reflect.Field jvm = runtime.getClass().getDeclaredField("jvm");
            jvm.setAccessible(true);
            sun.management.VMManagement mgmt =
                    (sun.management.VMManagement) jvm.get(runtime);
            java.lang.reflect.Method pid_method =
                    mgmt.getClass().getDeclaredMethod("getProcessId");
            pid_method.setAccessible(true);

            pid = (Integer) pid_method.invoke(mgmt);
            return pid;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the CPU load as a percentage from 0 to 100.
     *
     * @param cpu
     * @return
     */
    public static float[] cpuLoad(List<Integer> cpus) {
        StringBuffer sb = new StringBuffer("mpstat -P ");
        for (int k = 0; k < cpus.size(); k++) {
            if (k > 0) {
                sb.append(",");
            }
            sb.append(cpus.get(k));
        }
        sb.append(" 1 1");
        System.out.println("command:" + sb);
        String result = Bash.bash(sb.toString());
        System.out.println("results:" + result);
        String[] lines = result.split("\n");
        float[] f = new float[cpus.size()];
        for(int k=0;k<cpus.size();k++){
            String line= lines[lines.length-1-k];
            System.out.println(line);
            String[] columns = line.split(" ");
            f[k] = 100 - parseFloat(columns[columns.length - 1]);
        }
        return f;

    }
}
