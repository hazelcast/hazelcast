/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.ISemaphore;

import java.util.concurrent.Semaphore;

/**
 * Contains configuration options for the CP {@link ISemaphore}
 */
public class CPSemaphoreConfig {

    /**
     * Default value for JDK compatibility mode of the CP {@link ISemaphore}
     */
    public static final boolean DEFAULT_SEMAPHORE_JDK_COMPATIBILITY = false;


    /**
     * Name of the CP ISemaphore
     */
    private String name;

    /**
     * Enables / disables JDK compatibility of the CP {@link ISemaphore}.
     * When it is JDK compatible, just as in the {@link Semaphore#release()}
     * method, a permit can be released without acquiring it first, because
     * acquired permits are not bound to threads. However, there is no
     * auto-cleanup of acquired permits upon Hazelcast server / client failures.
     * If a permit holder fails, its permits must be released manually.
     * When JDK compatibility is disabled, a {@link HazelcastInstance} must
     * acquire permits before releasing them and it cannot release a permit
     * that it has not acquired. It means, you can acquire a permit
     * from one thread and release it from another thread using the same
     * {@link HazelcastInstance}, but not different {@link HazelcastInstance}s.
     * In this mode, acquired permits are automatically released upon failure
     * of the holder {@link HazelcastInstance}. So there is a minor behavioral
     * difference to the {@link Semaphore#release()} method.
     * <p>
     * JDK compatibility is disabled by default.
     */
    private boolean jdkCompatible = DEFAULT_SEMAPHORE_JDK_COMPATIBILITY;

    public CPSemaphoreConfig() {
        super();
    }

    public CPSemaphoreConfig(String name) {
        this.name = name;
    }

    public CPSemaphoreConfig(String name, boolean jdkCompatible) {
        this.name = name;
        this.jdkCompatible = jdkCompatible;
    }

    CPSemaphoreConfig(CPSemaphoreConfig config) {
        this.name = config.name;
        this.jdkCompatible = config.jdkCompatible;
    }

    /**
     * Returns name of the CP semaphore
     */
    public String getName() {
        return name;
    }

    /**
     * Sets name of the semaphore
     */
    public CPSemaphoreConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Returns true if the CP {@link ISemaphore} is compatible with
     * its JDK counterpart
     */
    public boolean isJDKCompatible() {
        return jdkCompatible;
    }

    /**
     * Sets JDK compatibility mode of the CP {@link ISemaphore}
     */
    public CPSemaphoreConfig setJDKCompatible(boolean jdkCompatible) {
        this.jdkCompatible = jdkCompatible;
        return this;
    }

    @Override
    public String toString() {
        return "CPSemaphoreConfig{" + "name='" + name + ", jdkCompatible=" + jdkCompatible + '\'' + '}';
    }
}
