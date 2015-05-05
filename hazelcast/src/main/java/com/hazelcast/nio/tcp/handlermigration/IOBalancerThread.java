package com.hazelcast.nio.tcp.handlermigration;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.EmptyStatement;

import java.util.concurrent.TimeUnit;

class IOBalancerThread extends Thread {
    private static final String THREAD_NAME_PREFIX = "IOBalancerThread";

    private final IOBalancer ioBalancer;
    private final ILogger log;
    private final int migrationIntervalSeconds;

    IOBalancerThread(IOBalancer ioBalancer, int migrationIntervalSeconds,
                             HazelcastThreadGroup threadGroup, ILogger log) {
        super(threadGroup.getInternalThreadGroup(), threadGroup.getThreadNamePrefix(THREAD_NAME_PREFIX));
        this.ioBalancer = ioBalancer;
        this.log = log;
        this.migrationIntervalSeconds = migrationIntervalSeconds;
    }

    @Override
    public void run() {
        try {
            log.finest("Starting IOBalancer thread");
            while (!Thread.interrupted()) {
                ioBalancer.checkReadHandlers();
                ioBalancer.checkWriteHandlers();
                TimeUnit.SECONDS.sleep(migrationIntervalSeconds);
            }
        } catch (InterruptedException e) {
            log.finest("IOBalancer thread stopped");
            //this thread is about to exit, no reason restoring the interrupt flag
            EmptyStatement.ignore(e);
        }
    }
}
