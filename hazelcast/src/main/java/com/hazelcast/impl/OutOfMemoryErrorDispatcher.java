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

package com.hazelcast.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OutOfMemoryHandler;

/**
 * @mdogan 8/16/12
 */
public final class OutOfMemoryErrorDispatcher {

    private static final HazelcastInstance[] instances = new HazelcastInstance[50];
    private static int size = 0;
    private static OutOfMemoryHandler handler = new DefaultOutOfMemoryHandler();

    public synchronized static void setHandler(OutOfMemoryHandler outOfMemoryHandler) {
        OutOfMemoryErrorDispatcher.handler = outOfMemoryHandler;
    }

    synchronized static boolean register(FactoryImpl factory) {
        if (size < instances.length - 1) {
            instances[size++] = factory;
            return true;
        }
        return false;
    }

    synchronized static boolean deregister(FactoryImpl factory) {
        for (int index = 0; index < instances.length; index++) {
            HazelcastInstance hz = instances[index];
            if (hz == factory) {
                try {
                    int numMoved = size - index - 1;
                    if (numMoved > 0) {
                        System.arraycopy(instances, index + 1, instances, index, numMoved);
                    }
                    instances[--size] = null;
                    return true;
                } catch (Throwable ignored) {
                }
            }
        }
        return false;
    }

    synchronized static void clear() {
        for (int i = 0; i < instances.length; i++) {
            instances[i] = null;
            size = 0;
        }
    }

    public synchronized static void onOutOfMemory(OutOfMemoryError oom) {
        if (handler != null) {
            handler.onOutOfMemory(oom, instances);
        }
    }

    private static class DefaultOutOfMemoryHandler extends OutOfMemoryHandler {

        public void onOutOfMemory(final OutOfMemoryError oom, final HazelcastInstance[] hazelcastInstances) {
            for (HazelcastInstance instance : hazelcastInstances) {
                if (instance != null) {
                    Helper.tryCloseConnections(instance);
                    Helper.tryStopThreads(instance);
                    Helper.tryShutdown(instance);
                }
            }
        }
    }

    public static final class Helper {

        public static void tryCloseConnections(final HazelcastInstance hazelcastInstance) {
            if (hazelcastInstance == null) return;
            final FactoryImpl factory = (FactoryImpl) hazelcastInstance;
            closeSockets(factory);
        }

        private static void closeSockets(final FactoryImpl factory) {
            if (factory.node.connectionManager != null) {
                try {
                    factory.node.connectionManager.shutdown();
                } catch (Throwable ignored) {
                }
            }
        }

        public static void tryShutdown(final HazelcastInstance hazelcastInstance) {
            if (hazelcastInstance == null) return;
            final FactoryImpl factory = (FactoryImpl) hazelcastInstance;
            closeSockets(factory);
            try {
                factory.node.doShutdown(true);
            } catch (Throwable ignored) {
            }
        }

        public static void inactivate(final HazelcastInstance hazelcastInstance) {
            if (hazelcastInstance == null) return;
            final FactoryImpl factory = (FactoryImpl) hazelcastInstance;
            factory.node.onOutOfMemory();
        }

        public static void tryStopThreads(final HazelcastInstance hazelcastInstance) {
            if (hazelcastInstance == null) return;
            final FactoryImpl factory = (FactoryImpl) hazelcastInstance;
            try {
                factory.node.threadGroup.interrupt();
                factory.node.executorManager.stop();
                factory.node.clusterService.stop();
            } catch (Throwable ignored) {
            }
        }
    }

    private OutOfMemoryErrorDispatcher() {}
}
