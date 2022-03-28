package com.hazelcast.spi.impl.reactor;


import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.internal.util.ThreadAffinityHelper;
import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.net.SocketAddress;
import java.util.BitSet;
import java.util.concurrent.Future;

public abstract class Reactor extends Thread {

    private BitSet allowedCpus;

    public Reactor(String name) {
        super(name);
    }

    public void setThreadAffinity(ThreadAffinity threadAffinity) {
        this.allowedCpus = threadAffinity.nextAllowedCpus();
    }

    public abstract void wakeup();

    public abstract void enqueue(Request request);

    public abstract Future<Channel> enqueue(SocketAddress address, Connection connection);

    protected void setThreadAffinity() {
        if (allowedCpus == null) {
            return;
        }

        ThreadAffinityHelper.setAffinity(allowedCpus);
        BitSet actualCpus = ThreadAffinityHelper.getAffinity();
        ILogger logger = Logger.getLogger(Reactor.class);
        if (!actualCpus.equals(allowedCpus)) {
            logger.warning(getName() + " affinity was not applied successfully. "
                    + "Expected CPUs:" + allowedCpus + ". Actual CPUs:" + actualCpus);
        } else {
            logger.info(getName() + " has affinity for CPUs:" + allowedCpus);
        }
    }
}
