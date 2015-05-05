/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
