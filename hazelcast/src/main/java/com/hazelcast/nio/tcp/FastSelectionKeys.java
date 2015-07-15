/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp;

import com.hazelcast.logging.ILogger;

import java.lang.reflect.Field;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.Class.forName;
import static java.lang.System.arraycopy;

/**
 * The FastSelectionKeys optimized the Selector so less litter is being created. The Selector uses a Set, but this creates
 * an object for every add of a selection key. With this FastSelectionKeys an array is being used since every key is going
 * to be inserted only once.
 *
 * This trick comes from Netty.
 */
public final class FastSelectionKeys extends AbstractSet<SelectionKey> {
    static final int INITIAL_CAPACITY = 32;
    static final String SELECTOR_IMPL = "sun.nio.ch.SelectorImpl";

    // Using this property the FastSelectionKeys optimization can be disabled. It is enabled by default.
    private static final boolean DISABLED = Boolean.getBoolean("hazelcast.io.fastselectionkeys.disabled");


    // the active SelectionKeys is the one where is being added to.
    SelectionKeys activeKeys = new SelectionKeys();
    // the passive SelectionKeys is one that is being read using the iterator.
    SelectionKeys passiveKeys = new SelectionKeys();
    private final IteratorImpl iterator = new IteratorImpl();

    FastSelectionKeys() {
    }

    @Override
    public boolean add(SelectionKey o) {
        return activeKeys.add(o);
    }

    @Override
    public int size() {
        return activeKeys.size;
    }

    @Override
    public Iterator<SelectionKey> iterator() {
        iterator.init(flip());
        return iterator;
    }

    private SelectionKey[] flip() {
        SelectionKeys tmp = activeKeys;
        activeKeys = passiveKeys;
        passiveKeys = tmp;

        activeKeys.size = 0;
        return passiveKeys.keys;
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    /**
     * Tries to optimize the provided Selector.
     *
     * @param selector
     * @return an FastSelectionKeySet if the optimization was a success, null otherwise.
     */
    public static FastSelectionKeys optimize(Selector selector, ILogger logger) {
        checkNotNull(selector, "selector");
        checkNotNull(logger, "logger");

        // optionally disable FastSelectionKey.
        if (DISABLED) {
            logger.info("FastSelectionKeys optimization has been disabled.");
            return null;
        }

        try {
            FastSelectionKeys selectedKeySet = new FastSelectionKeys();

            Class<?> selectorImplClass = findOptimizableSelectorClass(selector);
            if (selectorImplClass == null) {
                return null;
            }

            Field selectedKeysField = selectorImplClass.getDeclaredField("selectedKeys");
            selectedKeysField.setAccessible(true);

            Field publicSelectedKeysField = selectorImplClass.getDeclaredField("publicSelectedKeys");
            publicSelectedKeysField.setAccessible(true);

            selectedKeysField.set(selector, selectedKeySet);
            publicSelectedKeysField.set(selector, selectedKeySet);

            logger.finest("Injected FastSelectionKeys into " + selector.getClass().getName());

            return selectedKeySet;
        } catch (Throwable t) {
            // we don't want to print a warning because it could very well be that the target JVM doesn't
            // support this FastSelectionKeys optimization.
            logger.finest("Failed to inject FastSelectionKeys into " + selector.getClass().getName(), t);
            return null;
        }
    }

    static Class<?> findOptimizableSelectorClass(Selector selector) throws ClassNotFoundException {
        Class<?> selectorImplClass = forName(SELECTOR_IMPL, false, FastSelectionKeys.class.getClassLoader());

        // Ensure the current selector implementation is what we can instrument.
        if (!selectorImplClass.isAssignableFrom(selector.getClass())) {
            return null;
        }
        return selectorImplClass;
    }

    static final class SelectionKeys {

        SelectionKey[] keys = new SelectionKey[INITIAL_CAPACITY];
        int size;

        private boolean add(SelectionKey key) {
            if (key == null) {
                return false;
            }

            ensureCapacity();
            keys[size] = key;
            size++;
            return true;
        }

        private void ensureCapacity() {
            if (size < keys.length) {
                return;
            }

            SelectionKey[] newKeys = new SelectionKey[keys.length * 2];
            arraycopy(keys, 0, newKeys, 0, size);
            keys = newKeys;
        }
    }

    static final class IteratorImpl implements Iterator<SelectionKey> {

        SelectionKey[] keys;
        int index;

        private void init(SelectionKey[] keys) {
            this.keys = keys;
            this.index = -1;
        }

        @Override
        public boolean hasNext() {
            if (index >= keys.length - 1) {
                return false;
            }

            return keys[index + 1] != null;
        }

        @Override
        public SelectionKey next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            index++;
            return keys[index];
        }

        @Override
        public void remove() {
            if (index == -1 || index >= keys.length - 1 || keys[index] == null) {
                throw new IllegalStateException();
            }

            keys[index] = null;
        }
    }
}
