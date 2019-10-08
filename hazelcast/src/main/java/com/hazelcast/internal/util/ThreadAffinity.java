package com.hazelcast.internal.util;

import net.openhft.affinity.Affinity;

import java.util.concurrent.ConcurrentMap;

import static java.lang.Float.parseFloat;

public class ThreadAffinity {

    private static volatile int pid = 0;
    private static volatile String javaDir = null;
    private final static ConcurrentMap<Long,Integer> tids = new ConcurrentReferenceHashMap<>();

    public static void main(String[] args) {

        System.out.println(getTid(Thread.currentThread()));
    }

    public static String javaDirectory(){
        if(javaDir!=null){
            return javaDir;
        }

        String javaHome = System.getProperty("java.home");
        javaDir = javaHome.substring(0,javaHome.length()-3);
        return javaDir;
    }

    public static void setThreadAffinity(Thread t, int cpu) {
        Affinity.getThreadId();
    }

    public static int getTid(Thread t) {
        Integer tid = tids.get(t.getId());
        if(tid!=null){
            return tid;
        }

        String result = Bash.bash(javaDirectory()+"bin/jcmd "+getPid()+" Thread.print");
        String[] lines = result.split("\n");
        for(String line: lines){
            if(line.startsWith("\""+t.getName()+"\""))
                System.out.println(line);
            }
        }
        return 1;
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
    public static float cpuLoad(int cpu) {
        String cmd = Bash.bash("mpstat -P " + cpu);
        String[] lines = cmd.split("\n");
        String lastLine = lines[lines.length - 1];
        String[] columns = lastLine.split(" ");
        return 100 - parseFloat(columns[columns.length - 1]);
    }
}
