package com.hazelcast.spi.impl.proxyservice.impl;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.util.ExceptionUtil;

public class DistributedObjectFuture {

    volatile DistributedObject proxy;
    volatile Throwable error;

    boolean isSet() {
        return proxy != null;
    }

    public DistributedObject get() {
        if (proxy != null) {
            return proxy;
        }

        if (error != null) {
            throw ExceptionUtil.rethrow(error);
        }

        boolean interrupted = false;
        synchronized (this) {
            while (proxy == null && error == null) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        if (proxy != null) {
            return proxy;
        }
        throw ExceptionUtil.rethrow(error);
    }

    void set(DistributedObject o) {
        if (o == null) {
            throw new IllegalArgumentException("Proxy should not be null!");
        }
        synchronized (this) {
            proxy = o;
            notifyAll();
        }
    }

    void setError(Throwable t) {
        if (t == null) {
            throw new IllegalArgumentException("Error should not be null!");
        }
        if (proxy != null) {
            throw new IllegalStateException("Proxy is already set! Proxy: " + proxy + ", error: " + t);
        }
        synchronized (this) {
            error = t;
            notifyAll();
        }
    }
}
