package com.hazelcast.spi.impl.reactor;

import com.hazelcast.internal.util.counters.SwCounter;
import io.netty.incubator.channel.uring.IO_UringChannel;

import java.util.HashMap;
import java.util.Map;

import static java.lang.System.currentTimeMillis;

public final class MonitorThread extends Thread {

    private final Reactor[] reactors;
    private volatile boolean shutdown = false;
    private long prevMillis = currentTimeMillis();
    // There is a memory leak on the counters. When channels die, counters are not removed.
    private final Map<SwCounter, Prev> prevMap = new HashMap<>();


    public MonitorThread(Reactor[] reactors) {
        super("MonitorThread");
        this.reactors = reactors;
    }

    static class Prev {
        public long value;
    }

    @Override
    public void run() {
        try {
            long nextWakeup = currentTimeMillis() + 1000;
            while (!shutdown) {
                try {
                    long now = currentTimeMillis();
                    long remaining = nextWakeup - now;
                    if (remaining > 0) {
                        Thread.sleep(remaining);
                    }
                } catch (InterruptedException e) {
                    return;
                }

                nextWakeup += 1000;

                long currentMillis = currentTimeMillis();
                long elapsed = currentMillis - prevMillis;
                this.prevMillis = currentMillis;

                for (Reactor reactor : reactors) {
                    for (Channel channel : reactor.channels()) {
                        displayChannel(channel, elapsed);
                    }
                }

                for (Reactor reactor : reactors) {
                    displayReactor(reactor, elapsed);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void displayReactor(Reactor reactor, long elapsed) {
        System.out.println(reactor + " processed-request-count:" + reactor.processedRequest.get());
        System.out.println(reactor + " channel-count:" + reactor.channels().size());

        long processedRequest = reactor.processedRequest.get();
        Prev prevProcessedRequest = getPrev(reactor.processedRequest);
        long packetsReadDelta = processedRequest - prevProcessedRequest.value;
        System.out.println(reactor + " " + thp(packetsReadDelta, elapsed) + " processed-requests/second");
        prevProcessedRequest.value = processedRequest;
    }

    private void displayChannel(Channel channel, long elapsed) {
        if (channel instanceof IO_UringChannel) {
            IO_UringChannel u = (IO_UringChannel) channel;
            System.out.println(channel.remoteAddress + " scheduled:" + u.scheduled + " pending:" + u.pending.size());
        }

        long packetsRead = channel.framesRead.get();
        Prev prevPacketsRead = getPrev(channel.framesRead);
        long packetsReadDelta = packetsRead - prevPacketsRead.value;
        System.out.println(channel.remoteAddress + " " + thp(packetsReadDelta, elapsed) + " packets/second");
        prevPacketsRead.value = packetsRead;

        long bytesRead = channel.bytesRead.get();
        Prev prevBytesRead = getPrev(channel.bytesRead);
        long bytesReadDelta = bytesRead - prevBytesRead.value;
        System.out.println(channel.remoteAddress + " " + thp(bytesReadDelta, elapsed) + " bytes-read/second");
        prevBytesRead.value = bytesRead;

        long bytesWritten = channel.bytesWritten.get();
        Prev prevBytesWritten = getPrev(channel.bytesWritten);
        long bytesWrittenDelta = bytesWritten - prevBytesWritten.value;
        System.out.println(channel.remoteAddress + " " + thp(bytesWrittenDelta, elapsed) + " bytes-written/second");
        prevBytesWritten.value = bytesWritten;

        long handleOutboundCalls = channel.handleOutboundCalls.get();
        Prev prevHandleOutboundCalls = getPrev(channel.handleOutboundCalls);
        long handleOutboundCallsDelta = handleOutboundCalls - prevHandleOutboundCalls.value;
        System.out.println(channel.remoteAddress + " " + thp(handleOutboundCallsDelta, elapsed) + " handleOutbound-calls/second");
        prevHandleOutboundCalls.value = handleOutboundCalls;

        long readEvents = channel.readEvents.get();
        Prev prevReadEvents = getPrev(channel.readEvents);
        long readEventsDelta = readEvents - prevReadEvents.value;
        System.out.println(channel.remoteAddress + " " + thp(readEventsDelta, elapsed) + " read-events/second");
        prevReadEvents.value = readEvents;

        System.out.println(channel.remoteAddress + " " + (packetsReadDelta * 1.0f / (handleOutboundCallsDelta + 1)) + " packets/handleOutbound-call");
        System.out.println(channel.remoteAddress + " " + (packetsReadDelta * 1.0f / (readEventsDelta + 1)) + " packets/read-events");
        System.out.println(channel.remoteAddress + " " + (bytesReadDelta * 1.0f / (readEventsDelta + 1)) + " bytes-read/read-events");
    }

    private Prev getPrev(SwCounter counter) {
        Prev prev = prevMap.get(counter);
        if (prev == null) {
            prev = new Prev();
            prevMap.put(counter, prev);
        }
        return prev;
    }

    public static float thp(long delta, long elapsed) {
        return (delta * 1000f) / elapsed;
    }

    public void shutdown() {
        shutdown = true;
        interrupt();
    }
}
