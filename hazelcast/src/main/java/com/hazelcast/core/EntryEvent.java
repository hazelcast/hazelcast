/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Member;
import com.hazelcast.map.AbstractIMapEvent;
import com.hazelcast.map.IMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Map Entry event.
 *
 * @param <K> key of the map entry
 * @param <V> value of the map entry
 * @see com.hazelcast.core.EntryListener
 * @see com.hazelcast.map.listener.MapListener
 * @see IMap#addEntryListener(com.hazelcast.map.listener.MapListener, boolean)
 */
@SuppressFBWarnings("SE_BAD_FIELD")
public class EntryEvent<K, V> extends AbstractIMapEvent {

    private static final long serialVersionUID = -2296203982913729851L;

    protected K key;
    protected V oldValue;
    protected V value;
    protected V mergingValue;

    /**
     * Constructs an entry event.
     *
     * @param source    The object on which the event initially occurred
     * @param member    The interface to the cluster member (node)
     * @param eventType The event type as an enum {@link EntryEventType} integer
     * @param key       The key for this entry event
     * @param value     The value of the entry event
     * @throws IllegalArgumentException if source is {@code null}
     */
    public EntryEvent(Object source, Member member, int eventType, K key, V value) {
        this(source, member, eventType, key, null, value);
    }

    /**
     * Constructs an entry event.
     *
     * @param source    The object on which the Event initially occurred
     * @param member    The interface to the cluster member (node)
     * @param eventType The event type as an enum {@link EntryEventType} integer
     * @param key       The key of this entry event
     * @param oldValue  The old value of the entry event
     * @param value     The value of the entry event
     * @throws IllegalArgumentException if source is {@code null}
     */
    public EntryEvent(Object source, Member member, int eventType, K key, V oldValue, V value) {
        super(source, member, eventType);
        this.key = key;
        this.oldValue = oldValue;
        this.value = value;
    }

    /**
     * Constructs an entry event.
     *
     * @param source       The object on which the Event initially occurred
     * @param member       The interface to the cluster member (node)
     * @param eventType    The event type as an enum {@link EntryEventType} integer
     * @param key          The key of this entry event
     * @param oldValue     The old value of the entry event
     * @param value        The value of the entry event
     * @param mergingValue The incoming merging value of the entry event
     * @throws IllegalArgumentException if source is {@code null}
     */
    public EntryEvent(Object source, Member member, int eventType, K key, V oldValue, V value, V mergingValue) {
        super(source, member, eventType);
        this.key = key;
        this.oldValue = oldValue;
        this.value = value;
        this.mergingValue = mergingValue;
    }

    /**
     * Returns the key of the entry event.
     *
     * @return the key of the entry event
     */
    public K getKey() {
        return key;
    }

    /**
     * Returns the old value of the entry event.
     *
     * @return the old value of the entry event
     */
    public V getOldValue() {
        return this.oldValue;
    }

    /**
     * Returns the value of the entry event.
     *
     * @return the value of the entry event
     */
    public V getValue() {
        return value;
    }

    /**
     * Returns the incoming merging value of the entry event.
     *
     * @return the merging value
     */
    public V getMergingValue() {
        return mergingValue;
    }

    @Override
    public String toString() {
        return "EntryEvent{"
                + super.toString()
                + ", key=" + getKey()
                + ", oldValue=" + getOldValue()
                + ", value=" + getValue()
                + ", mergingValue=" + getMergingValue()
                + '}';
    }
}
