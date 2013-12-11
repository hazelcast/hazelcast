package com.hazelcast.util.executor;

import java.util.concurrent.ThreadFactory;

public class NamedThreadFactory implements ThreadFactory {

    private final String name;

    public NamedThreadFactory(final String name) {
        this.name = name;
    }

    public Thread newThread(final Runnable r) {
        final Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }
}
