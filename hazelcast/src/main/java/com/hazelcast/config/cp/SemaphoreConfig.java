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

package com.hazelcast.config.cp;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.ISemaphore;

import java.util.concurrent.Semaphore;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;

/**
 * Contains configuration options for CP {@link ISemaphore}
 */
public class SemaphoreConfig {

    /**
     * The default value for the JDK compatibility mode
     * of CP {@link ISemaphore}
     */
    public static final boolean DEFAULT_SEMAPHORE_JDK_COMPATIBILITY = false;

    /**
     * Default value for the initial permit count of Semaphores.
     */
    public static final int DEFAULT_INITIAL_PERMITS = 0;


    /**
     * Name of CP ISemaphore
     */
    private String name;

    /**
     * Enables / disables JDK compatibility of CP {@link ISemaphore}.
     * When it is JDK compatible, just as in the {@link Semaphore#release()}
     * method, a permit can be released without acquiring it first, because
     * acquired permits are not bound to threads. However, there is no
     * auto-cleanup mechanism for acquired permits upon Hazelcast
     * server / client failures. If a permit holder fails, its permits must be
     * released manually. When JDK compatibility is disabled,
     * a {@link HazelcastInstance} must acquire permits before releasing them
     * and it cannot release a permit that it has not acquired. It means, you
     * can acquire a permit from one thread and release it from another thread
     * using the same {@link HazelcastInstance}, but not different
     * {@link HazelcastInstance}s. In this mode, acquired permits are
     * automatically released upon failure of the holder
     * {@link HazelcastInstance}. So there is a minor behavioral difference
     * to the {@link Semaphore#release()} method.
     * <p>
     * JDK compatibility is disabled by default.
     */
    private boolean jdkCompatible = DEFAULT_SEMAPHORE_JDK_COMPATIBILITY;

    /**
     * Number of permits to initialize the Semaphore. If a positive value is
     * set, the Semaphore is initialized with the given number of permits.
     */
    private int initialPermits = DEFAULT_INITIAL_PERMITS;

    public SemaphoreConfig() {
        super();
    }

    public SemaphoreConfig(String name) {
        this.name = name;
    }

    public SemaphoreConfig(String name, boolean jdkCompatible, int initialPermits) {
        this.name = name;
        this.jdkCompatible = jdkCompatible;
        this.initialPermits = initialPermits;
    }

    SemaphoreConfig(SemaphoreConfig config) {
        this.name = config.name;
        this.jdkCompatible = config.jdkCompatible;
        this.initialPermits = config.initialPermits;
    }

    /**
     * Returns the name of CP ISemaphore
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of CP ISemaphore
     */
    public SemaphoreConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Returns true if CP ISemaphore is compatible with
     * its JDK counterpart
     */
    public boolean isJDKCompatible() {
        return jdkCompatible;
    }

    /**
     * Sets JDK compatibility mode of CP ISemaphore
     */
    public SemaphoreConfig setJDKCompatible(boolean jdkCompatible) {
        this.jdkCompatible = jdkCompatible;
        return this;
    }

    /**
     * Returns initial permit count of the Semaphore
     */
    public int getInitialPermits() {
        return initialPermits;
    }

    /**
     * Sets initial permit count of the Semaphore
     */
    public SemaphoreConfig setInitialPermits(int initialPermits) {
        checkNotNegative(initialPermits, "initial permits cannot be negative");
        this.initialPermits = initialPermits;
        return this;
    }

    @Override
    public String toString() {
        return "SemaphoreConfig{" + "name='" + name + '\'' + ", jdkCompatible=" + jdkCompatible + ", initialPermits="
                + initialPermits + '}';
    }
}
