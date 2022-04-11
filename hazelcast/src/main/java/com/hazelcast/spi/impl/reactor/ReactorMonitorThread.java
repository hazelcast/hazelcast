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
    private final Map<SwCounter, LongHolder> prevMap = new HashMap<>();

    public ReactorMonitorThread(Reactor[] reactors) {
        super("MonitorThread");
        this.reactors = reactors;
    }

    static class LongHolder {
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
        log(reactor + " request-count:" + reactor.requests.get());
        log(reactor + " channel-count:" + reactor.channels().size());

        long requests = reactor.requests.get();
        LongHolder prevRequests = getPrev(reactor.requests);
        long requestsDelta = requests - prevRequests.value;
        log(reactor + " " + thp(requestsDelta, elapsed) + " requests/second");
        prevRequests.value = requests;
    }

    private void log(String s) {
        System.out.println("[monitor] " + s);
    }

    private void monitor(Channel channel, long elapsed) {
        long packetsRead = channel.framesRead.get();
        LongHolder prevPacketsRead = getPrev(channel.framesRead);
        long packetsReadDelta = packetsRead - prevPacketsRead.value;
        log(channel + " " + thp(packetsReadDelta, elapsed) + " packets/second");
        prevPacketsRead.value = packetsRead;

        long bytesRead = channel.bytesRead.get();
        LongHolder prevBytesRead = getPrev(channel.bytesRead);
        long bytesReadDelta = bytesRead - prevBytesRead.value;
        log(channel + " " + thp(bytesReadDelta, elapsed) + " bytes-read/second");
        prevBytesRead.value = bytesRead;

        long bytesWritten = channel.bytesWritten.get();
        LongHolder prevBytesWritten = getPrev(channel.bytesWritten);
        long bytesWrittenDelta = bytesWritten - prevBytesWritten.value;
        log(channel + " " + thp(bytesWrittenDelta, elapsed) + " bytes-written/second");
        prevBytesWritten.value = bytesWritten;

        long handleOutboundCalls = channel.handleWriteCnt.get();
        LongHolder prevHandleOutboundCalls = getPrev(channel.handleWriteCnt);
        long handleOutboundCallsDelta = handleOutboundCalls - prevHandleOutboundCalls.value;
        log(channel + " " + thp(handleOutboundCallsDelta, elapsed) + " handleOutbound-calls/second");
        prevHandleOutboundCalls.value = handleOutboundCalls;

        long readEvents = channel.readEvents.get();
        LongHolder prevReadEvents = getPrev(channel.readEvents);
        long readEventsDelta = readEvents - prevReadEvents.value;
        log(channel + " " + thp(readEventsDelta, elapsed) + " read-events/second");
        prevReadEvents.value = readEvents;

        log(channel + " " + (packetsReadDelta * 1.0f / (handleOutboundCallsDelta + 1)) + " packets/handleOutbound-call");
        log(channel + " " + (packetsReadDelta * 1.0f / (readEventsDelta + 1)) + " packets/read-events");
        log(channel + " " + (bytesReadDelta * 1.0f / (readEventsDelta + 1)) + " bytes-read/read-events");

        if (packetsReadDelta == 0) {
            if (channel instanceof NioChannel) {
                NioChannel nioChannel = (NioChannel) channel;
                boolean hasData = !nioChannel.unflushedFrames.isEmpty() || !nioChannel.ioVector.isEmpty();
                //if (nioChannel.flushThread.get() == null && hasData) {
                log(channel + " is stuck: unflushed-frames:" + nioChannel.unflushedFrames.size()
                        + " ioVector.empty:" + nioChannel.ioVector.isEmpty()
                        + " flushed:" + nioChannel.flushThread.get()
                        + " reactor.contains:" + nioChannel.reactor.publicRunQueue.contains(nioChannel));
                //}
            }
        }
    }

    private LongHolder getPrev(SwCounter counter) {
        LongHolder prev = prevMap.get(counter);
        if (prev == null) {
            prev = new LongHolder();
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
