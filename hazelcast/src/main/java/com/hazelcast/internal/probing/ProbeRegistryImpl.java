package com.hazelcast.internal.probing;

import static java.lang.System.currentTimeMillis;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ProbeRegistryImpl implements ProbeRegistry, Runnable, Closeable {

    private final Set<ProbeSource> sources = ConcurrentHashMap.newKeySet();
    private final ProbeingCycle cycle;
    private final long cycleTime;
    private final Thread rendering;
    private volatile boolean run = true; 

    public ProbeRegistryImpl(ProbeRenderer renderer, long cycleTime) {
        this(new ProbingCycleImpl(renderer), cycleTime);
    }
    
    public ProbeRegistryImpl(ProbeingCycle cycle, long cycleTime) {
        this.cycle = cycle;
        this.cycleTime = cycleTime;
        this.rendering = new Thread(this);
        rendering.setDaemon(true);
        rendering.start();
    }

    @Override
    public void register(ProbeSource source) {
        sources.add(source);
    }

    @Override
    public void run() {
        while (run) {
            long start = currentTimeMillis();
            for (ProbeSource s : sources) {
                s.probeIn(cycle);
            }
            long sleepTime = cycleTime - (currentTimeMillis() - start);
            if (sleepTime > 0) {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    // if registry got closed the loop ends and the thread terminates
                }
            }
        }
    }

    @Override
    public void close() {
        run = false;
        rendering.interrupt();
    }
}
