/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.util.ExceptionUtil;

public class DistributedObjectFuture {

    private volatile DistributedObject proxy;
    private volatile Throwable error;

    // non-initialized distributed object
    private volatile DistributedObject rawProxy;

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

        boolean interrupted = waitUntilSetAndInitialized();

        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        if (proxy != null) {
            return proxy;
        }
        throw ExceptionUtil.rethrow(error);
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
            throw new IllegalArgumentException("Proxy should not be null!");
        }
        synchronized (this) {
            if (!initialized && o instanceof InitializingObject) {
                rawProxy = o;
            } else {
                proxy = o;
            }
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
