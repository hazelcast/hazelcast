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
import com.hazelcast.util.EmptyStatement;

import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.util.ValidationUtil.isNotNull;
import static java.lang.System.arraycopy;

public final class OutOfMemoryErrorDispatcher {

    private static final int MAX_REGISTERED_INSTANCES = 50;
    private static final HazelcastInstance[] EMPTY_INSTANCES = new HazelcastInstance[0];

    private static final AtomicReference<HazelcastInstance[]> SERVER_INSTANCES_REF =
            new AtomicReference<HazelcastInstance[]>(EMPTY_INSTANCES);

    private static final AtomicReference<HazelcastInstance[]> CLIENT_INSTANCES_REF =
            new AtomicReference<HazelcastInstance[]>(EMPTY_INSTANCES);

    private static volatile OutOfMemoryHandler handler = new DefaultOutOfMemoryHandler();

    private static volatile OutOfMemoryHandler clientHandler = new EmptyOutOfMemoryHandler();


    private OutOfMemoryErrorDispatcher() {
    }

    //for testing only
    static HazelcastInstance[] current() {
        return SERVER_INSTANCES_REF.get();
    }

    public static void setServerHandler(OutOfMemoryHandler outOfMemoryHandler) {
        handler = outOfMemoryHandler;
    }

    public static void setClientHandler(OutOfMemoryHandler outOfMemoryHandler) {
        clientHandler = outOfMemoryHandler;
    }

    public static void registerServer(HazelcastInstance instance) {
       register(SERVER_INSTANCES_REF, instance);
    }

    public static void registerClient(HazelcastInstance instance) {
        register(CLIENT_INSTANCES_REF, instance);
    }

    private static void register(AtomicReference<HazelcastInstance[]> ref, HazelcastInstance instance) {
        isNotNull(instance, "instance");

        for (;;) {
            HazelcastInstance[] oldInstances = ref.get();
            if (oldInstances.length == MAX_REGISTERED_INSTANCES) {
                return;
            }

            HazelcastInstance[] newInstances = new HazelcastInstance[oldInstances.length + 1];
            arraycopy(oldInstances, 0, newInstances, 0, oldInstances.length);
            newInstances[oldInstances.length] = instance;

            if (ref.compareAndSet(oldInstances, newInstances)) {
                return;
            }
        }
    }

    public static void deregisterServer(HazelcastInstance instance) {
        deregister(SERVER_INSTANCES_REF, instance);
    }

    public static void deregisterClient(HazelcastInstance instance) {
        deregister(CLIENT_INSTANCES_REF, instance);
    }

    private static void deregister(AtomicReference<HazelcastInstance[]> ref, HazelcastInstance instance) {
        isNotNull(instance, "instance");

        for (;;) {
            HazelcastInstance[] oldInstances = ref.get();
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

            if (ref.compareAndSet(oldInstances, newInstances)) {
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

    public static void clearServers() {
        SERVER_INSTANCES_REF.set(EMPTY_INSTANCES);
    }

    public static void clearClients() {
        CLIENT_INSTANCES_REF.set(EMPTY_INSTANCES);
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

        OutOfMemoryHandler h = clientHandler;
        if (h != null && h.shouldHandle(outOfMemoryError)) {
            try {
                HazelcastInstance[] clients = removeRegisteredClients();
                h.onOutOfMemory(outOfMemoryError, clients);
            } catch (Throwable ignored) {
                EmptyStatement.ignore(ignored);
            }
        }

        h = handler;
        if (h != null && h.shouldHandle(outOfMemoryError)) {
            try {
                HazelcastInstance[] instances = removeRegisteredServers();
                h.onOutOfMemory(outOfMemoryError, instances);
            } catch (Throwable ignored) {
                EmptyStatement.ignore(ignored);
            }
        }
    }

    private static HazelcastInstance[] removeRegisteredServers() {
        return removeRegisteredInstances(SERVER_INSTANCES_REF);
    }

    private static HazelcastInstance[] removeRegisteredClients() {
        return removeRegisteredInstances(CLIENT_INSTANCES_REF);
    }

    private static HazelcastInstance[] removeRegisteredInstances(AtomicReference<HazelcastInstance[]> ref) {
        for (;;) {
            HazelcastInstance[] instances = ref.get();
            if (ref.compareAndSet(instances, EMPTY_INSTANCES)) {
                return instances;
            }
        }
    }

    private static class EmptyOutOfMemoryHandler extends OutOfMemoryHandler {
        @Override
        public void onOutOfMemory(OutOfMemoryError oome, HazelcastInstance[] hazelcastInstances) {
        }

        @Override
        public boolean shouldHandle(OutOfMemoryError oome) {
            return false;
        }
    }

}
