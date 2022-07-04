/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.impl.proxyservice.impl;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.spi.impl.InitializingObject;

import java.util.UUID;
import java.util.concurrent.ForkJoinPool;

public class DistributedObjectFuture
        implements ForkJoinPool.ManagedBlocker {

    private volatile DistributedObject proxy;
    private volatile Throwable error;

    // non-initialized distributed object
    private volatile DistributedObject rawProxy;

    private final UUID source;

    public DistributedObjectFuture(UUID source) {
        this.source = source;
    }

    boolean isSetAndInitialized() {
        return proxy != null || error != null;
    }

    public DistributedObject get() {
        if (proxy != null) {
            return proxy;
        }

        if (error != null) {
            throw ExceptionUtil.rethrow(error);
        }

        // Ensure sufficient parallelism if
        // caller thread is a ForkJoinPool thread
        try {
            ForkJoinPool.managedBlock(this);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            error = e;
        }

        if (proxy != null) {
            return proxy;
        }

        throw ExceptionUtil.rethrow(error);
    }

    @Override
    public boolean block() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()
                || isReleasable()) {
            return true;
        }
        if (waitUntilSetAndInitialized()) {
            Thread.currentThread().interrupt();
            return true;
        }
        return true;
    }

    @Override
    public boolean isReleasable() {
        return proxy != null;
    }

    public UUID getSource() {
        return source;
    }

    public DistributedObject getNow() {
        if (error != null) {
            throw ExceptionUtil.rethrow(error);
        }
        // return completed proxy or null
        return proxy;
    }

    private boolean waitUntilSetAndInitialized() {
        boolean interrupted = false;
        synchronized (this) {
            while (proxy == null && error == null) {
                if (rawProxy != null) {
                    initialize();
                    break;
                }
                try {
                    wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        return interrupted;
    }

    private void initialize() {
        synchronized (this) {
            try {
                InitializingObject o = (InitializingObject) rawProxy;
                o.initialize();
                proxy = rawProxy;
            } catch (Throwable e) {
                error = e;
            }
            notifyAll();
        }
    }

    void set(DistributedObject o, boolean initialized) {
        if (o == null) {
            throw new IllegalArgumentException("Proxy must not be null!");
        }
        synchronized (this) {
            if (error == null) {
                if (!initialized && o instanceof InitializingObject) {
                    rawProxy = o;
                } else {
                    proxy = o;
                }
            }
            notifyAll();
        }
    }

    void setError(Throwable t) {
        if (t == null) {
            throw new IllegalArgumentException("Error must not be null!");
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
