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

package com.hazelcast.instance;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OutOfMemoryHandler;

import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.util.ValidationUtil.isNotNull;
import static java.lang.System.arraycopy;

public final class OutOfMemoryErrorDispatcher {

    private static final int MAX_REGISTERED_INSTANCES = 50;
    private static final HazelcastInstance[] EMPTY_INSTANCES = new HazelcastInstance[0];

    private static final AtomicReference<HazelcastInstance[]> INSTANCES_REF =
            new AtomicReference<HazelcastInstance[]>(EMPTY_INSTANCES);

    private static volatile OutOfMemoryHandler handler = new DefaultOutOfMemoryHandler();

    private static volatile OutOfMemoryHandler clientHandler;

    private OutOfMemoryErrorDispatcher() {
    }

    //for testing only
    static HazelcastInstance[] current() {
        return INSTANCES_REF.get();
    }

    public static void setHandler(OutOfMemoryHandler outOfMemoryHandler) {
        handler = outOfMemoryHandler;
    }

    public static void setClientHandler(OutOfMemoryHandler outOfMemoryHandler) {
        clientHandler = outOfMemoryHandler;
    }

    public static void register(HazelcastInstance instance) {
        isNotNull(instance, "instance");

        for (;;) {
            HazelcastInstance[] oldInstances = INSTANCES_REF.get();
            if (oldInstances.length == MAX_REGISTERED_INSTANCES) {
                return;
            }

            HazelcastInstance[] newInstances = new HazelcastInstance[oldInstances.length + 1];
            arraycopy(oldInstances, 0, newInstances, 0, oldInstances.length);
            newInstances[oldInstances.length] = instance;

            if (INSTANCES_REF.compareAndSet(oldInstances, newInstances)) {
                return;
            }
        }
    }

    public static void deregister(HazelcastInstance instance) {
        isNotNull(instance, "instance");

        for (;;) {
            HazelcastInstance[] oldInstances = INSTANCES_REF.get();
            int indexOf = indexOf(oldInstances, instance);
            if (indexOf == -1) {
                return;
            }

            HazelcastInstance[] newInstances;
            if (oldInstances.length == 1) {
                newInstances = EMPTY_INSTANCES;
            } else {
                newInstances = new HazelcastInstance[oldInstances.length - 1];
                arraycopy(oldInstances, 0, newInstances, 0, indexOf);
                if (indexOf < newInstances.length) {
                    arraycopy(oldInstances, indexOf + 1, newInstances, indexOf, newInstances.length - indexOf);
                }
            }

            if (INSTANCES_REF.compareAndSet(oldInstances, newInstances)) {
                return;
            }
        }
    }

    private static int indexOf(HazelcastInstance[] instances, HazelcastInstance instance) {
        for (int k = 0; k < instances.length; k++) {
            if (instance == instances[k]) {
                return k;
            }
        }
        return -1;
    }

    static void clear() {
        INSTANCES_REF.set(EMPTY_INSTANCES);
    }

    public static void inspectOutputMemoryError(Throwable throwable) {
        if (throwable == null) {
            return;
        }

        if (throwable instanceof OutOfMemoryError) {
            onOutOfMemory((OutOfMemoryError) throwable);
        }
    }

    /**
     * Signals the OutOfMemoryErrorDispatcher that an OutOfMemoryError happened.
     * <p/>
     * If there are any registered instances, they are automatically unregistered. This is done to prevent creating
     * new objects during the deregistration process while the system is suffering from a shortage of memory.
     *
     * @param outOfMemoryError the out of memory error
     */
    public static void onOutOfMemory(OutOfMemoryError outOfMemoryError) {
        isNotNull(outOfMemoryError, "outOfMemoryError");

        HazelcastInstance[] instances = removeRegisteredInstances();
        if (instances.length == 0) {
            return;
        }

        OutOfMemoryHandler h = clientHandler;
        if (h != null) {
            try {
                h.onOutOfMemory(outOfMemoryError, instances);
            } catch (Throwable ignored) {
            }
        }

        h = handler;
        if (h != null) {
            try {
                h.onOutOfMemory(outOfMemoryError, instances);
            } catch (Throwable ignored) {
            }
        }
    }

    private static HazelcastInstance[] removeRegisteredInstances() {
        for (;;) {
            HazelcastInstance[] instances = INSTANCES_REF.get();
            if (INSTANCES_REF.compareAndSet(instances, EMPTY_INSTANCES)) {
                return instances;
            }
        }
    }

    private static class DefaultOutOfMemoryHandler extends OutOfMemoryHandler {

        @Override
        public void onOutOfMemory(OutOfMemoryError oom, HazelcastInstance[] hazelcastInstances) {
            for (HazelcastInstance instance : hazelcastInstances) {
                if (instance instanceof HazelcastInstanceImpl) {
                    Helper.tryCloseConnections(instance);
                    Helper.tryStopThreads(instance);
                    Helper.tryShutdown(instance);
                }
            }
            System.err.println(oom);
        }
    }

    public static final class Helper {

        private Helper() {
        }

        public static void tryCloseConnections(HazelcastInstance hazelcastInstance) {
            if (hazelcastInstance == null) {
                return;
            }
            HazelcastInstanceImpl factory = (HazelcastInstanceImpl) hazelcastInstance;
            closeSockets(factory);
        }

        private static void closeSockets(HazelcastInstanceImpl factory) {
            if (factory.node.connectionManager != null) {
                try {
                    factory.node.connectionManager.shutdown();
                } catch (Throwable ignored) {
                }
            }
        }

        public static void tryShutdown(HazelcastInstance hazelcastInstance) {
            if (hazelcastInstance == null) {
                return;
            }
            HazelcastInstanceImpl factory = (HazelcastInstanceImpl) hazelcastInstance;
            closeSockets(factory);
            try {
                factory.node.shutdown(true);
            } catch (Throwable ignored) {
            }
        }

        public static void inactivate(HazelcastInstance hazelcastInstance) {
            if (hazelcastInstance == null) {
                return;
            }
            final HazelcastInstanceImpl factory = (HazelcastInstanceImpl) hazelcastInstance;
            factory.node.inactivate();
        }

        public static void tryStopThreads(HazelcastInstance hazelcastInstance) {
            if (hazelcastInstance == null) {
                return;
            }

            HazelcastInstanceImpl factory = (HazelcastInstanceImpl) hazelcastInstance;
            try {
                factory.node.threadGroup.interrupt();
            } catch (Throwable ignored) {
            }
        }
    }
}
