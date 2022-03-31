package io.netty.incubator.channel.uring;


import com.hazelcast.spi.impl.reactor.Channel;
import com.hazelcast.spi.impl.reactor.Reactor;
import com.hazelcast.spi.impl.reactor.nio.NioChannel;

public final class MonitorThread extends Thread {

    private final Reactor[] reactors;
    private volatile boolean shutdown = false;

    public MonitorThread(Reactor[] reactors) {
        this.reactors = reactors;
    }

    public void run() {
        try {
            while (!shutdown) {

                for (Reactor reactor : reactors) {
                    for (Channel c : reactor.channels()) {
                        displayChannel(c);
                    }
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void displayChannel(Channel c) {
        if (c instanceof NioChannel) {
            NioChannel channel = (NioChannel) c;
            long packetsRead = channel.packetsRead;
            float deltaPacketsRead = packetsRead - channel.prevPacketsRead;
            System.out.println(channel.remoteAddress + " " + (deltaPacketsRead) + " packets/second");
            channel.prevPacketsRead = packetsRead;

            long bytesRead = channel.bytesRead;
            float deltaBytesRead = bytesRead - channel.prevBytesRead;
            System.out.println(channel.remoteAddress + " " + (deltaBytesRead) + " bytes-read/second");
            channel.prevBytesRead = bytesRead;
        } else if (c instanceof IO_UringChannel) {
            IO_UringChannel channel = (IO_UringChannel) c;
            long packetsRead = channel.packetsRead;
            float deltaPacketsRead = packetsRead - channel.prevPacketsRead;
            System.out.println(channel.remoteAddress + " " + (deltaPacketsRead) + " packets/second");
            channel.prevPacketsRead = packetsRead;

            long bytesRead = channel.bytesRead;
            float deltaBytesRead = bytesRead - channel.prevBytesRead;
            System.out.println(channel.remoteAddress + " " + (deltaBytesRead) + " bytes-read/second");
            channel.prevBytesRead = bytesRead;
        } else {
            throw new IllegalArgumentException();
        }
    }

    public void shutdown() {
        shutdown = true;
        interrupt();
    }
}
