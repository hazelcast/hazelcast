package com.hazelcast.spi.impl.reactor;

import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.spi.impl.reactor.nio.NioChannel;

import java.util.HashMap;
import java.util.Map;

import static java.lang.System.currentTimeMillis;

public final class ReactorMonitorThread extends Thread {

    private final Reactor[] reactors;
    private volatile boolean shutdown = false;
    private long prevMillis = currentTimeMillis();
    // There is a memory leak on the counters. When channels die, counters are not removed.
    private final Map<SwCounter, Prev> prevMap = new HashMap<>();

    public ReactorMonitorThread(Reactor[] reactors) {
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
                        monitor(channel, elapsed);
                    }
                }

                for (Reactor reactor : reactors) {
                    monitor(reactor, elapsed);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void monitor(Reactor reactor, long elapsed) {
        println(reactor + " request-count:" + reactor.requests.get());
        println(reactor + " channel-count:" + reactor.channels().size());

        long requests = reactor.requests.get();
        Prev prevRequests = getPrev(reactor.requests);
        long requestsDelta = requests - prevRequests.value;
        println(reactor + " " + thp(requestsDelta, elapsed) + " requests/second");
        prevRequests.value = requests;
    }


    private void println(String s) {
        System.out.println("[monitor] " + s);
    }

    private void monitor(Channel channel, long elapsed) {
        long packetsRead = channel.framesRead.get();
        Prev prevPacketsRead = getPrev(channel.framesRead);
        long packetsReadDelta = packetsRead - prevPacketsRead.value;
        println(channel + " " + thp(packetsReadDelta, elapsed) + " packets/second");
        prevPacketsRead.value = packetsRead;

        long bytesRead = channel.bytesRead.get();
        Prev prevBytesRead = getPrev(channel.bytesRead);
        long bytesReadDelta = bytesRead - prevBytesRead.value;
        println(channel + " " + thp(bytesReadDelta, elapsed) + " bytes-read/second");
        prevBytesRead.value = bytesRead;

        long bytesWritten = channel.bytesWritten.get();
        Prev prevBytesWritten = getPrev(channel.bytesWritten);
        long bytesWrittenDelta = bytesWritten - prevBytesWritten.value;
        println(channel + " " + thp(bytesWrittenDelta, elapsed) + " bytes-written/second");
        prevBytesWritten.value = bytesWritten;

        long handleOutboundCalls = channel.handleWriteCnt.get();
        Prev prevHandleOutboundCalls = getPrev(channel.handleWriteCnt);
        long handleOutboundCallsDelta = handleOutboundCalls - prevHandleOutboundCalls.value;
        println(channel + " " + thp(handleOutboundCallsDelta, elapsed) + " handleOutbound-calls/second");
        prevHandleOutboundCalls.value = handleOutboundCalls;

        long readEvents = channel.readEvents.get();
        Prev prevReadEvents = getPrev(channel.readEvents);
        long readEventsDelta = readEvents - prevReadEvents.value;
        println(channel + " " + thp(readEventsDelta, elapsed) + " read-events/second");
        prevReadEvents.value = readEvents;

        println(channel + " " + (packetsReadDelta * 1.0f / (handleOutboundCallsDelta + 1)) + " packets/handleOutbound-call");
        println(channel + " " + (packetsReadDelta * 1.0f / (readEventsDelta + 1)) + " packets/read-events");
        println(channel + " " + (bytesReadDelta * 1.0f / (readEventsDelta + 1)) + " bytes-read/read-events");

        if (packetsReadDelta == 0) {
            if (channel instanceof NioChannel) {
                NioChannel nioChannel = (NioChannel) channel;
                boolean hasData = !nioChannel.unflushedFrames.isEmpty() || !nioChannel.ioVector.isEmpty();
                //if (nioChannel.flushThread.get() == null && hasData) {
                println(channel + " is stuck: unflushed-frames:" + nioChannel.unflushedFrames.size() + " ioVector.empty:" + nioChannel.ioVector.isEmpty() + " flushed:" + nioChannel.flushThread.get()+" reactor.contains:"+nioChannel.reactor.publicRunQueue.contains(nioChannel));
                //}
            }
        }
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
