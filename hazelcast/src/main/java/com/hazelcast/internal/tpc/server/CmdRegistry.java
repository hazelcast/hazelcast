/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpc.server;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

/**
 * A registry where all {@link Cmd} that can be performed, are registered.
 * <p/>
 * The {@link CmdRegistry} is used by the {@link CmdAllocator} to load Commands
 * with some command id.
 */
public class CmdRegistry {

    private final AtomicReferenceArray<Entry> entries;
    private final int capacity;

    public CmdRegistry(int capacity) {
        this.capacity = checkPositive(capacity, "capacity");
        this.entries = new AtomicReferenceArray<>(new Entry[capacity]);
    }

    public void register(int cmdId, Class<? extends Cmd> clazz) {
        checkNotNegative(cmdId, "cmdId");
        checkNotNull(clazz, "clazz");
        register(cmdId, loadConstructor(clazz));
    }

    private static Supplier<? extends Cmd> loadConstructor(Class<? extends Cmd> clazz) {
        try {
            Constructor<? extends Cmd> constructor = clazz.getConstructor();
            return () -> {
                try {
                    return constructor.newInstance();
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            };
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public void register(int cmdId, Supplier<? extends Cmd> supplier) {
        checkNotNegative(cmdId, "cmdId");
        checkNotNull(supplier, "supplier");

        Entry entry = new Entry(cmdId, supplier);
        if (!entries.compareAndSet(cmdId, null, entry)) {
            //todo: improve exception
            throw new IllegalStateException();
        }
        entries.set(cmdId, entry);
    }

    public int capacity() {
        return capacity;
    }

    /**
     * Gets the entry with the given cmdId. Could be <code>null</code> if the entry
     * doesn't exist for the given cmdId.
     *
     * @param cmdId the if of the command of the entry to look for.
     * @return the Entry.
     */
    public Entry get(int cmdId) {
        checkNotNegative(cmdId, "cmdId");

        if (cmdId >= capacity) {
            return null;
        }

        return entries.get(cmdId);
    }

    public static class Entry {
        private final Supplier<? extends Cmd> supplier;
        private final int cmdId;

        private Entry(int cmdId, Supplier<? extends Cmd> supplier) {
            this.cmdId = cmdId;
            this.supplier = supplier;
        }

        public Supplier<? extends Cmd> getSupplier() {
            return supplier;
        }

        public int getCmdId() {
            return cmdId;
        }
    }
}
