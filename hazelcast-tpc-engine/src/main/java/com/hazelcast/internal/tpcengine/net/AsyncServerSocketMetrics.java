/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.net;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Contains the metrics for an {@link AsyncServerSocket}.
 */
@SuppressWarnings("checkstyle:ConstantName")
public class AsyncServerSocketMetrics {

    private static final VarHandle ACCEPTED;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            ACCEPTED = l.findVarHandle(AsyncServerSocketMetrics.class, "accepted", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private volatile long accepted;

    /**
     * Returns the number of accepted sockets.
     *
     * @return the number of accepted sockets.
     */
    public long accepted() {
        return (long) ACCEPTED.getOpaque(this);
    }

    /**
     * Increases the number of accepted sockets by 1.
     */
    public void incAccepted() {
         ACCEPTED.setOpaque(this, (long) ACCEPTED.getOpaque(this) + 1);
    }
}
