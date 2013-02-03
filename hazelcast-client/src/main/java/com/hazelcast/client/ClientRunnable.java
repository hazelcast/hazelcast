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

package com.hazelcast.client;

public abstract class ClientRunnable implements Runnable {
    protected volatile boolean running = true;
    protected volatile boolean terminated = false;
    protected final Object monitor = new Object();

    protected abstract void customRun() throws InterruptedException;

    public void run() {
        try {
            while (running) {
                try {
                    customRun();
                } catch (InterruptedException e) {
                    return;
                }
            }
        } finally {
            terminate();
        }
    }

    public void shutdown() {
        if (terminated) {
            return;
        }
        synchronized (monitor) {
            running = false;
            while (!terminated) {
                try {
                    monitor.wait();
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    protected void terminate() {
        synchronized (monitor) {
            terminated = true;
            monitor.notifyAll();
        }
    }
}
