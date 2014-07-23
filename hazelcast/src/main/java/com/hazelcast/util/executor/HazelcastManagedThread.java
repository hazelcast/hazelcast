/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
 * @author sozal 7/23/14
 */
public class HazelcastManagedThread extends Thread {

    public HazelcastManagedThread() {
    }

    public HazelcastManagedThread(Runnable target) {
        super(target);
    }

    public HazelcastManagedThread(ThreadGroup group, Runnable target) {
        super(group, target);
    }

    public HazelcastManagedThread(String name) {
        super(name);
    }

    public HazelcastManagedThread(ThreadGroup group, String name) {
        super(group, name);
    }

    public HazelcastManagedThread(Runnable target, String name) {
        super(target, name);
    }

    public HazelcastManagedThread(ThreadGroup group, Runnable target, String name) {
        super(group, target, name);
    }

    @Override
    public void setContextClassLoader(ClassLoader cl) {
        // Set only if specified classloader is not empty, otherwise go one with current
        if (cl != null) {
            super.setContextClassLoader(cl);
        }
    }

    protected void beforeRun() {

    }

    protected void executeRun() {
        super.run();
    }

    protected void afterRun() {

    }

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
