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

package com.hazelcast.core;

/**
 * Map Entry event.
 *
 * @param <K> key of the map entry
 * @param <V> value of the map entry
 * @see com.hazelcast.core.EntryListener
 * @see com.hazelcast.core.IMap#addEntryListener(EntryListener, boolean)
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings("SE_BAD_FIELD")
public class EntryEvent<K, V> extends AbstractIMapEvent {

    private static final long serialVersionUID = -2296203982913729851L;

    protected K key;

    protected V oldValue;

    protected V value;


    public EntryEvent(Object source, Member member, int eventType, K key, V value) {
        this(source, member, eventType, key, null, value);
    }

    public EntryEvent(Object source, Member member, int eventType, K key, V oldValue, V value) {
        super(source, member, eventType);
        this.key = key;
        this.oldValue = oldValue;
        this.value = value;
    }

    /**
     * Returns the key of the entry event
     *
     * @return the key
     */
    public K getKey() {
        return key;
    }

    /**
     * Returns the old value of the entry event
     *
     * @return old value.
     */
    public V getOldValue() {
        return this.oldValue;
    }

    /**
     * Returns the value of the entry event
     *
     * @return value.
     */
    public V getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "EntryEvent{"
                + super.toString()
                + ", key=" + key
                + ", oldValue=" + oldValue
                + ", value=" + value
                + '}';
    }
}
