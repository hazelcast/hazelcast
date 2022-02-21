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

package com.hazelcast.instance.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OutOfMemoryHandler;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.internal.util.MemoryInfoAccessor;
import com.hazelcast.internal.util.RuntimeMemoryInfoAccessor;

import static com.hazelcast.internal.util.EmptyStatement.ignore;

/**
 * Default OutOfMemoryHandler implementation that tries to release local resources (threads, connections, memory)
 * immediately and disconnects members from the rest of the cluster.
*/
public class DefaultOutOfMemoryHandler extends OutOfMemoryHandler {

    /*
     * Known OOME error messages:
     *
     * java.lang.OutOfMemoryError: Java heap space
     * java.lang.OutOfMemoryError: PermGen space
     * java.lang.OutOfMemoryError: GC overhead limit exceeded
     * java.lang.OutOfMemoryError: unable to create new native thread
     * java.lang.OutOfMemoryError: nativeGetNewTLA
     * java.lang.OutOfMemoryError: Requested array size exceeds VM limit
     * java.lang.OutOfMemoryError: request <size> bytes for <reason>. Out of swap space?
     * java.lang.OutOfMemoryError: <reason> <stack trace> (Native method)
     */

    public static final String FREE_MAX_PERCENTAGE_PROP = "hazelcast.oome.handler.free_max.percentage";

    static final String GC_OVERHEAD_LIMIT_EXCEEDED = "GC overhead limit exceeded";
    private static final int HUNDRED_PERCENT = 100;
    private static final int FIFTY_PERCENT = 50;
    private static final long MAX_TOTAL_DELTA = MemoryUnit.MEGABYTES.toBytes(1);
    private static final double FREE_MAX_RATIO;

    static {
        int percentage = Integer.parseInt(System.getProperty(FREE_MAX_PERCENTAGE_PROP, "10"));
        if (percentage < 1 || percentage > FIFTY_PERCENT) {
            throw new IllegalArgumentException("'" + FREE_MAX_PERCENTAGE_PROP
                    + "' should be in [1, 50] range! Current: " + percentage);
        }
        FREE_MAX_RATIO = ((double) percentage) / HUNDRED_PERCENT;
    }

    private final double freeVersusMaxRatio;
    private final MemoryInfoAccessor memoryInfoAccessor;

    public DefaultOutOfMemoryHandler() {
        this(FREE_MAX_RATIO);
    }

    public DefaultOutOfMemoryHandler(double freeVersusMaxRatio) {
        this(freeVersusMaxRatio, new RuntimeMemoryInfoAccessor());
    }

    public DefaultOutOfMemoryHandler(double freeVersusMaxRatio, MemoryInfoAccessor memoryInfoAccessor) {
        this.freeVersusMaxRatio = freeVersusMaxRatio;
        this.memoryInfoAccessor = memoryInfoAccessor;
    }

    @Override
    public void onOutOfMemory(OutOfMemoryError oome, HazelcastInstance[] hazelcastInstances) {
        for (HazelcastInstance instance : hazelcastInstances) {
            if (instance instanceof HazelcastInstanceImpl) {
                OutOfMemoryHandlerHelper.tryCloseConnections(instance);
                OutOfMemoryHandlerHelper.tryShutdown(instance);
            }
        }
        try {
            oome.printStackTrace(System.err);
        } catch (Throwable ignored) {
            ignore(ignored);
        }
    }

    @Override
    public boolean shouldHandle(OutOfMemoryError oome) {
        try {
            if (GC_OVERHEAD_LIMIT_EXCEEDED.equals(oome.getMessage())) {
                return true;
            }

            long maxMemory = memoryInfoAccessor.getMaxMemory();
            long totalMemory = memoryInfoAccessor.getTotalMemory();

            // if total-memory has not reached to max-memory
            // then no need to handle this
            if (totalMemory < maxMemory - MAX_TOTAL_DELTA) {
                return false;
            }

            // since previous total vs max memory comparison
            // freeMemory should return the same result
            // with = (maxMemory - totalMemory + freeMemory)
            long freeMemory = memoryInfoAccessor.getFreeMemory();
            if (freeMemory > maxMemory * freeVersusMaxRatio) {
                return false;
            }

        } catch (Throwable ignored) {
            ignore(ignored);
        }

        return true;
    }
}
