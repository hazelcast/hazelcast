package com.hazelcast;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public class StageLatencyMonitor {

    public static ConcurrentMap<String, List<StageLatencyMonitor>> monitors = new ConcurrentHashMap<>();

    static {
        new DisplayThread().start();
    }

    private static class DisplayThread extends Thread {

        DisplayThread() {
            super("StageLatencyMonitorThread");
            setDaemon(true);
        }

        @Override
        public void run() {
            StringBuffer sb = new StringBuffer();

            for (; ; ) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    return;
                }

                for (Map.Entry<String, List<StageLatencyMonitor>> entry : monitors.entrySet()) {
                    long totalTimeNs = 0;
                    long totalCount = 0;

                    String groupName = entry.getKey();
                    List<StageLatencyMonitor> groupMonitors = entry.getValue();
                    for (StageLatencyMonitor m : groupMonitors) {
                        totalCount += m.count;
                        totalTimeNs += m.nanoTime;
                    }

                    if (totalCount == 0) {
                        sb.append(groupName).append(" no measurements\n");
                    } else {
                        long averageLatency = totalTimeNs / totalCount;
                        sb.append(groupName).append(" average latency:").append(averageLatency).append(" ns\n");
                    }
                }

                System.out.println(sb);
                sb.setLength(0);
            }
        }
    }

    private final AtomicLongFieldUpdater<StageLatencyMonitor> NANO_TIME = newUpdater(StageLatencyMonitor.class, "nanoTime");
    private final AtomicLongFieldUpdater<StageLatencyMonitor> COUNT = newUpdater(StageLatencyMonitor.class, "count");

    private volatile long nanoTime;
    private volatile long count;

    public static StageLatencyMonitor newInstance(String group) {
        StageLatencyMonitor monitor = new StageLatencyMonitor();
        synchronized (group) {
            List<StageLatencyMonitor> oldList = monitors.get(group);
            List<StageLatencyMonitor> newList = oldList == null ? new ArrayList<>() : new ArrayList<>(oldList);
            newList.add(monitor);
            monitors.put(group, newList);
        }
        return monitor;
    }

    public void record(long startNanos) {
        COUNT.lazySet(this, count + 1);
        NANO_TIME.lazySet(this, nanoTime + (System.nanoTime() - startNanos));
    }
}
