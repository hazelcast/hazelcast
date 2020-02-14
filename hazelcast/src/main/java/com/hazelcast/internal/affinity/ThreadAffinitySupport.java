/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.affinity;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.GroupProperty;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.affinity.ThreadAffinity.Group.IO;
import static com.hazelcast.internal.affinity.ThreadAffinity.Group.PARTITION_THREAD;
import static com.hazelcast.internal.affinity.ThreadAffinityProperties.isAffinityEnabled;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.EmptyStatement.ignore;
import static com.hazelcast.util.OsHelper.OS;
import static com.hazelcast.util.OsHelper.isUnixFamily;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.disjoint;

public class ThreadAffinitySupport {

    public static volatile ThreadAffinitySupport INSTANCE = null;
    private static final AtomicBoolean INIT = new AtomicBoolean(false);

    private final ILogger logger;
    private ConcurrentMap<ThreadAffinity, ThreadAffinityController> registry = new ConcurrentHashMap<>();

    private ThreadAffinitySupport(ILogger logger) {
        this.logger = logger;
    }

    public ThreadAffinityController create(Class<? extends Thread> cls) {
        checkNotNull(cls);
        ThreadAffinity type = cls.getAnnotation(ThreadAffinity.class);
        if (type != null) {
            return getOrPutIfAbsent(registry, type, key -> new ThreadAffinityController(logger, type));
        }

        return null;
    }

    public static void init(ILogger logger) {
        if (!isAffinityEnabled()) {
            return;
        }

        if (!INIT.compareAndSet(false, true)) {
            throw new IllegalStateException("Thread affinity support disallows multiple Hazelcast instances under the same JVM");
        }

        if (!isUnixFamily()) {
            throw new IllegalStateException("Thread affinity not supported on this platform: " + OS);
        }

        logger.info("Thread affinity option detected, checking for dependencies");

        try {
            // Check if dependency OpenHFT Affinity is present in classpath
            Class.forName("net.openhft.affinity.AffinityLock");
        } catch (ClassNotFoundException e) {
            logger.warning("Dependency net.openhft:affinity not found in classpath. Affinity support is disabled.");
            return;
        }

        try {
            // Check if JNA is available in the classpath
            Class.forName("com.sun.jna.Platform");
        } catch (ClassNotFoundException e) {
            logger.warning("Dependency net.java.dev.jna:jna* not found in classpath. Affinity support is disabled.");
        }

        if (!disjoint(ThreadAffinityProperties.getCoreIds(IO), ThreadAffinityProperties.getCoreIds(PARTITION_THREAD))) {
            logger.warning("Affinity assignments for the different affinity groups have some cores in common (shared).");
        }

        failFastChecks();

        logger.info("Thread affinity dependencies available, support enabled");
        INSTANCE = new ThreadAffinitySupport(logger);
    }

    private static void failFastChecks() {
        // Available resources conflicts
        int totalAvailableCores = Runtime.getRuntime().availableProcessors();
        int partitionOpCores = ThreadAffinityProperties.countCores(PARTITION_THREAD);
        if (partitionOpCores > totalAvailableCores) {
            throw new IllegalStateException("Thread affinity core count for " + PARTITION_THREAD + " is set to high, "
                    + "more than the available core count on the system " + totalAvailableCores);
        }

        int ioCores = ThreadAffinityProperties.countCores(IO);
        if (ioCores > totalAvailableCores) {
            throw new IllegalStateException("Thread affinity core count for " + IO + " is set to high, "
                    + "more than the available core count on the system " + totalAvailableCores);
        }

        if (ioCores % 2 != 0) {
            throw new IllegalStateException("Thread affinity core count for " + IO + " is not an even number");
        }

        if (partitionOpCores + ioCores > totalAvailableCores) {
            throw new IllegalStateException("Thread affinity core sum for all affinity groups is to high, "
                    + "more than the available core count on the system " + totalAvailableCores);
        }

        // Other configuration conflicts
        try {
            int partOpCountSystemProperty = Integer.parseInt(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName());
            if (partOpCountSystemProperty != partitionOpCores) {
                throw new IllegalStateException("Thread affinity core count for " + PARTITION_THREAD
                        + " conflicts with system property " + GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName());
            }
        } catch (NumberFormatException ex) {
            ignore(ex);
        }

        try {
            int ioCountSystemProperty = Integer.parseInt(GroupProperty.IO_THREAD_COUNT.getName());
            if (ioCountSystemProperty != ioCores) {
                throw new IllegalStateException("Thread affinity core count for " + IO + " conflicts with system property "
                        + GroupProperty.IO_THREAD_COUNT.getName());
            }
        } catch (NumberFormatException ex) {
            ignore(ex);
        }

        try {
            int ioCountSystemProperty = Integer.parseInt(GroupProperty.IO_INPUT_THREAD_COUNT.getName());
            if (ioCountSystemProperty != ioCores / 2) {
                throw new IllegalStateException("Thread affinity core count for " + IO + " conflicts with system property "
                        + GroupProperty.IO_INPUT_THREAD_COUNT.getName());
            }
        } catch (NumberFormatException ex) {
            ignore(ex);
        }

        try {
            int ioCountSystemProperty = Integer.parseInt(GroupProperty.IO_OUTPUT_THREAD_COUNT.getName());
            if (ioCountSystemProperty != ioCores / 2) {
                throw new IllegalStateException("Thread affinity core count for " + IO + " conflicts with system property "
                        + GroupProperty.IO_OUTPUT_THREAD_COUNT.getName());
            }
        } catch (NumberFormatException ex) {
            ignore(ex);
        }
    }

}
