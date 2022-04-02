package com.hazelcast.spi.impl.reactor;

import io.netty.incubator.channel.uring.IO_UringChannel;

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
        if(channel instanceof IO_UringChannel ) {
            IO_UringChannel u = (IO_UringChannel) channel;
            System.out.println(channel.remoteAddress + " scheduled:" + u.scheduled +" pending:"+u.pending.size());
        }

        long packetsRead = channel.packetsRead;
        long packetsReadDelta = packetsRead - channel.prevPacketsRead;
        System.out.println(channel.remoteAddress + " " + thp(packetsReadDelta, elapsed) + " packets/second");
        channel.prevPacketsRead = packetsRead;

        long bytesRead = channel.bytesRead;
        long bytesReadDelta = bytesRead - channel.prevBytesRead;
        System.out.println(channel.remoteAddress + " " + thp(bytesReadDelta, elapsed) + " bytes-read/second");
        channel.prevBytesRead = bytesRead;

        long bytesWritten = channel.bytesWritten;
        long bytesWrittenDelta = channel.prevBytesWritten;
        System.out.println(channel.remoteAddress + " " + thp(bytesWrittenDelta, elapsed) + " bytes-written/second");
        channel.prevBytesWritten = bytesWritten;

        long handleOutboundCalls = channel.handleOutboundCalls;
        long handleOutboundCallsDelta = handleOutboundCalls - channel.prevHandleOutboundCalls;
        System.out.println(channel.remoteAddress + " " + thp(handleOutboundCallsDelta, elapsed) + " handleOutbound-calls/second");
        channel.prevHandleOutboundCalls = handleOutboundCalls;

        long readEvents = channel.readEvents;
        long readEventsDelta = readEvents - channel.prevReadEvents;
        System.out.println(channel.remoteAddress + " " + thp(readEventsDelta, elapsed) + " read-events/second");
        channel.prevReadEvents = readEvents;

        System.out.println(channel.remoteAddress + " " + (packetsReadDelta * 1.0f / (handleOutboundCallsDelta + 1)) + " packets/handleOutbound-call");
        System.out.println(channel.remoteAddress + " " + (packetsReadDelta * 1.0f / (readEventsDelta + 1)) + " packets/read-events");
        System.out.println(channel.remoteAddress + " " + (bytesReadDelta * 1.0f / (readEventsDelta + 1)) + " bytes-read/read-events");
        System.out.println(channel.remoteAddress + " " + (channel.bytesWritten-channel.bytesWrittenConfirmed) + " byte write diff");
    }

    public static float thp(long delta, long elapsed) {
        return (delta * 1000f) / elapsed;
    }

    public void shutdown() {
        shutdown = true;
        interrupt();
    }
}
