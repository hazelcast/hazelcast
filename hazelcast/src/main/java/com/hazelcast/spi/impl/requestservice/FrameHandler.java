package com.hazelcast.spi.impl.requestservice;

import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.spi.impl.engine.frame.Frame;
import com.hazelcast.spi.impl.engine.frame.FrameAllocator;

public class FrameHandler {

    //protected final OpScheduler scheduler;
    private final OpAllocator opAllocator = new OpAllocator();
    protected final SwCounter requests = SwCounter.newSwCounter();
    private final Managers managers;
    protected FrameAllocator localResponseFrameAllocator;
    protected FrameAllocator remoteResponseFrameAllocator;

    public FrameHandler(Managers managers){
        this.managers = managers;
        //this.scheduler = new OpScheduler(32768, Integer.MAX_VALUE);
    }

    public void handleResponse(Frame response){

    }
}
