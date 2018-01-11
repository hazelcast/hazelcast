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

package com.hazelcast.util.executor;

import com.hazelcast.instance.OutOfMemoryErrorDispatcher;

/**
 * Base class for all Hazelcast threads to manage them from a single point.
 * Concrete classes can customize their behaviours by overriding
 * {@link com.hazelcast.util.executor.HazelcastManagedThread#beforeRun},
 * {@link com.hazelcast.util.executor.HazelcastManagedThread#executeRun} and
 * {@link com.hazelcast.util.executor.HazelcastManagedThread#afterRun} methods.
 */
public class HazelcastManagedThread extends Thread {

    public HazelcastManagedThread() {
    }

    public HazelcastManagedThread(Runnable target) {
        super(target);
    }

    public HazelcastManagedThread(String name) {
        super(name);
    }

    public HazelcastManagedThread(Runnable target, String name) {
        super(target, name);
    }

    @Override
    public void setContextClassLoader(ClassLoader cl) {
        // Set only if specified classloader is not empty, otherwise go one with current
        if (cl != null) {
            super.setContextClassLoader(cl);
        }
    }

    /**
     * Called before inner run method and can be overridden to customize.
     */
    protected void beforeRun() {

    }

    /**
     * Does the actual run and can be overridden to customize.
     */
    protected void executeRun() {
        super.run();
    }

    /**
     * Called after inner run method and can be overridden to customize.
     */
    protected void afterRun() {

    }

    /**
     * Manages the thread lifecycle and can be overridden to customize if needed.
     */
    public void run() {
        try {
            beforeRun();
            executeRun();
        } catch (OutOfMemoryError e) {
            OutOfMemoryErrorDispatcher.onOutOfMemory(e);
        } finally {
            afterRun();
        }
    }
}
