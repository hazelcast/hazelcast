package io.netty.incubator.channel.uring;


import com.hazelcast.spi.impl.reactor.Channel;
import com.hazelcast.spi.impl.reactor.Reactor;
import com.hazelcast.spi.impl.reactor.nio.NioChannel;

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

    private void displayChannel(Channel c, long elapsed) {
        if (c instanceof NioChannel) {
            NioChannel channel = (NioChannel) c;
            long packetsRead = channel.packetsRead;
            System.out.println(channel.remoteAddress + " " + thp(packetsRead, channel.prevPacketsRead, elapsed) + " packets/second");
            channel.prevPacketsRead = packetsRead;

            long bytesRead = channel.bytesRead;
            System.out.println(channel.remoteAddress + " " + thp(bytesRead, channel.prevBytesRead, elapsed) + " bytes-read/second");
            channel.prevBytesRead = bytesRead;

            long handleCalls = channel.handleOutboundCalls;
            System.out.println(channel.remoteAddress + " " + thp(handleCalls, channel.prevHandleCalls, elapsed) + " handle-calls/second");
            channel.prevHandleCalls = handleCalls;

            long readEvents = channel.readEvents;
            System.out.println(channel.remoteAddress + " " + thp(readEvents, channel.prevReadEvents, elapsed) + " read-events/second");
            channel.prevReadEvents = readEvents;

            System.out.println(channel.remoteAddress + " " + (packetsRead * 1.0f / (handleCalls + 1)) + " packets/handle-call");
            System.out.println(channel.remoteAddress + " " + (packetsRead * 1.0f / (readEvents + 1)) + " packets/read-events");
        } else if (c instanceof IO_UringChannel) {
            IO_UringChannel channel = (IO_UringChannel) c;

            long packetsRead = channel.packetsRead;
            System.out.println(channel.remoteAddress + " " + thp(packetsRead, channel.prevPacketsRead, elapsed) + " packets/second");
            channel.prevPacketsRead = packetsRead;

            long bytesRead = channel.bytesRead;
            System.out.println(channel.remoteAddress + " " + thp(bytesRead, channel.prevBytesRead, elapsed) + " bytes-read/second");
            channel.prevBytesRead = bytesRead;

            long handleCalls = channel.handleOutboundCalls;
            System.out.println(channel.remoteAddress + " " + thp(handleCalls, channel.prevHandleCalls, elapsed) + " handle-calls/second");
            channel.prevHandleCalls = handleCalls;

            long readEvents = channel.readEvents;
            System.out.println(channel.remoteAddress + " " + thp(readEvents, channel.prevReadEvents, elapsed) + " read-events/second");
            channel.prevReadEvents = readEvents;

            System.out.println(channel.remoteAddress + " " + (packetsRead * 1.0f) / (handleCalls + 1) + " packets/handle-call");
            System.out.println(channel.remoteAddress + " " + (packetsRead * 1.0f / (readEvents + 1)) + " packets/read-events");
        } else {
            throw new IllegalArgumentException();
        }
    }

    public static float thp(long current, long previous, long elapsed) {
        return ((current - previous) * 1000f) / elapsed;
    }

    public void shutdown() {
        shutdown = true;
        interrupt();
    }
}
