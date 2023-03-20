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

package com.hazelcast.internal.tpcengine;

import com.hazelcast.internal.tpcengine.util.UnsafeLocator;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Contains the metrics for an {@link AsyncServerSocket}.
 */
@SuppressWarnings("checkstyle:ConstantName")
public class AsyncServerSocketMetrics {

    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;
    private static final long OFFSET_accepted;
    private volatile long accepted;

    static {
        try {
            OFFSET_accepted = getOffset("accepted");
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private static long getOffset(String fieldName) throws NoSuchFieldException {
        Field field = AsyncServerSocketMetrics.class.getDeclaredField(fieldName);
        return UNSAFE.objectFieldOffset(field);
    }

    /**
     * Returns the number of accepted sockets.
     *
     * @return the number of accepted sockets.
     */
    public long accepted() {
        return accepted;
    }

    /**
     * Increases the number of accepted sockets by 1.
     */
    public void incAccepted() {
        UNSAFE.putOrderedLong(this, OFFSET_accepted, accepted + 1);
    }
}
