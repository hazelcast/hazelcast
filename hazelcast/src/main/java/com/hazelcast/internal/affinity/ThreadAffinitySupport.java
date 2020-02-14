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
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.affinity.ThreadAffinity.Group.IO;
import static com.hazelcast.internal.affinity.ThreadAffinity.Group.PARTITION_THREAD;
import static com.hazelcast.internal.affinity.ThreadAffinityProperties.isAffinityEnabled;
import static com.hazelcast.spi.properties.GroupProperty.IO_INPUT_THREAD_COUNT;
import static com.hazelcast.spi.properties.GroupProperty.IO_OUTPUT_THREAD_COUNT;
import static com.hazelcast.spi.properties.GroupProperty.IO_THREAD_COUNT;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_OPERATION_THREAD_COUNT;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.EmptyStatement.ignore;
import static com.hazelcast.util.OsHelper.OS;
import static com.hazelcast.util.OsHelper.isUnixFamily;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.disjoint;

public final class ThreadAffinitySupport {

    private static final AtomicBoolean INIT = new AtomicBoolean(false);
    private static volatile ThreadAffinitySupport instance;

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

    public static ThreadAffinitySupport getInstance() {
        return instance;
    }

    public static void init(ILogger logger, HazelcastProperties properties) {
        if (!isAffinityEnabled()) {
            return;
        }

        if (!INIT.compareAndSet(false, true)) {
            throw new IllegalStateException("Thread affinity support disallows multiple Hazelcast instances under the same JVM");
        }

        logger.info("Thread affinity option detected, checking for dependencies");

        if (!isUnixFamily()) {
            throw new IllegalStateException("Thread affinity not supported on this platform: " + OS);
        }

        failFastMissingLibs();

        failFastConfigConflicts(properties);

        logger.info("Thread affinity dependencies available, support enabled");
        instance = new ThreadAffinitySupport(logger);
    }

    protected static void failFastMissingLibs() {
        try {
            // Check if dependency OpenHFT Affinity is present in classpath
            Class.forName("net.openhft.affinity.AffinityLock");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Dependency net.openhft:affinity not found in classpath.");
        }

        try {
            // Check if JNA is available in the classpath
            Class.forName("com.sun.jna.Platform");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Dependency net.java.dev.jna:jna* not found in classpath.");
        }
    }

    private static void failFastConfigConflicts(HazelcastProperties properties) {
        // Affinity conflicts
        if (!disjoint(ThreadAffinityProperties.getCoreIds(IO), ThreadAffinityProperties.getCoreIds(PARTITION_THREAD))) {
            throw new IllegalStateException("Affinity assignments for the different affinity groups "
                    + "have some cores in common (shared).");
        }

        // Available resources conflicts
        int totalAvailableCpus = Runtime.getRuntime().availableProcessors();
        int partitionOpCpus = ThreadAffinityProperties.countCores(PARTITION_THREAD);
        if (partitionOpCpus > totalAvailableCpus) {
            throw new IllegalStateException("Thread affinity core count for " + PARTITION_THREAD + " is set too high, "
                    + "more than the available core count on the system " + totalAvailableCpus);
        }

        int ioCpus = ThreadAffinityProperties.countCores(IO);
        if (ioCpus > totalAvailableCpus) {
            throw new IllegalStateException("Thread affinity core count for " + IO + " is set too high, "
                    + "more than the available core count on the system " + totalAvailableCpus);
        }

        if (ioCpus % 2 != 0) {
            throw new IllegalStateException("Thread affinity core count for " + IO + " is not an even number");
        }

        if (partitionOpCpus + ioCpus > totalAvailableCpus) {
            throw new IllegalStateException("Thread affinity core sum for all affinity groups is too high, "
                    + "more than the available core count on the system " + totalAvailableCpus);
        }

        // Other configuration conflicts
        failFastConfigMismatch(partitionOpCpus, properties, PARTITION_OPERATION_THREAD_COUNT, PARTITION_THREAD);

        failFastConfigMismatch(ioCpus, properties, IO_THREAD_COUNT, IO);

        failFastConfigMismatch(ioCpus / 2, properties, IO_INPUT_THREAD_COUNT, IO);

        failFastConfigMismatch(ioCpus / 2, properties, IO_OUTPUT_THREAD_COUNT, IO);
    }

    private static void failFastConfigMismatch(int affinityValue, HazelcastProperties properties,
                                               HazelcastProperty prop, ThreadAffinity.Group group) {
        try {
            int groupPropValue = Integer.parseInt(properties.get(prop.getName()));
            if (groupPropValue != affinityValue) {
                throw new IllegalStateException(
                        "Thread affinity core count for " + group + " conflicts with group property " + prop.getName());
            }
        } catch (NumberFormatException ex) {
            ignore(ex);
        }

        try {
            int systemPropValue = Integer.parseInt(prop.getSystemProperty());
            if (systemPropValue != affinityValue) {
                throw new IllegalStateException(
                        "Thread affinity core count for " + group + " conflicts with system property " + prop.getName());
            }
        } catch (NumberFormatException ex) {
            ignore(ex);
        }
    }

}
