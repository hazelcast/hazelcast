package com.hazelcast.spi.impl.reactor;

import static java.lang.System.currentTimeMillis;

public final class MonitorThread extends Thread {

    private final Reactor[] reactors;
    private volatile boolean shutdown = false;
    private long prevMillis = currentTimeMillis();

    public MonitorThread(Reactor[] reactors) {
        this.reactors = reactors;
    }

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
                    for (Channel c : reactor.channels()) {
                        displayChannel(c, elapsed);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void displayChannel(Channel channel, long elapsed) {
        long packetsRead = channel.packetsRead;
        System.out.println(channel.remoteAddress + " " + thp(packetsRead, channel.prevPacketsRead, elapsed) + " packets/second");
        channel.prevPacketsRead = packetsRead;

        long bytesRead = channel.bytesRead;
        System.out.println(channel.remoteAddress + " " + thp(bytesRead, channel.prevBytesRead, elapsed) + " bytes-read/second");
        channel.prevBytesRead = bytesRead;

        long bytesWritten = channel.bytesWritten;
        System.out.println(channel.remoteAddress + " " + thp(bytesWritten, channel.prevBytesWritten, elapsed) + " bytes-written/second");
        channel.prevBytesWritten = bytesWritten;

        long handleOutboundCalls = channel.handleOutboundCalls;
        System.out.println(channel.remoteAddress + " " + thp(handleOutboundCalls, channel.prevHandleOutboundCalls, elapsed) + " handleOutbound-calls/second");
        channel.prevHandleOutboundCalls = handleOutboundCalls;

        long readEvents = channel.readEvents;
        System.out.println(channel.remoteAddress + " " + thp(readEvents, channel.prevReadEvents, elapsed) + " read-events/second");
        channel.prevReadEvents = readEvents;

        System.out.println(channel.remoteAddress + " " + (packetsRead * 1.0f / (handleOutboundCalls + 1)) + " packets/handleOutbound-call");
        System.out.println(channel.remoteAddress + " " + (packetsRead * 1.0f / (readEvents + 1)) + " packets/read-events");
    }

    public static float thp(long current, long previous, long elapsed) {
        return ((current - previous) * 1000f) / elapsed;
    }

    public void shutdown() {
        shutdown = true;
        interrupt();
    }
}
